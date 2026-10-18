// Интеграционные тесты устойчивости очередей и планировщика (T-032;
// ТЗ 11.2 «Устойчивость», 3.4, 11.4 п. 9) против живых Redis
// и PostgreSQL dev-окружения: компоненты internal/queue (RedisQueue,
// Processing, Reliab.Claim с q:dead, BackpressureGate/WatermarkPublisher,
// MAXLEN) и internal/scheduler (DispatchBatch/EnqueueBatch/LeaseOnce).
//
// Сценарии:
//   - публикация → потребление → запись результата в БД → XACK
//     (Processing, T-025): сбой обработки не подтверждает запись, новый
//     потребитель после разрыва читает только новые записи (">"), а
//     зависшую возвращает клейм — повторная обработка завершает цикл;
//   - остановка воркера посреди пачки (ТЗ 11.2): неподтверждённые записи
//     возвращает XAUTOCLAIM с исходным телом, счётчик попыток растёт,
//     после MaxAttempts — q:dead (формат Dead) и XACK исходных; все
//     task_id переживают цикл;
//   - потеря Redis (ТЗ 3.4, 11.4 п. 9): задачи исчезают вместе со
//     стримами, домены «зависают» в queued — lease-проход (T-029)
//     возвращает их в pending, диспетчер (T-027) перевыбирает, постановка
//     (T-028) переиздаёт пачку; живой воркер восстанавливает группу
//     после NOGROUP; счёт и id-множество доменов не меняются;
//   - backpressure (T-026): ниже порога публикация проходит, на пороге
//     блокируется до освобождения ёмкости, отмена контекста — чистый
//     выход (graceful shutdown, ТЗ 3.4);
//   - MAXLEN (T-025): публикация сверх предела обрезает стрим — длина
//     не растёт с числом публикаций, старые записи уходят.
//
// Имена стримов — боевые константы queue.Stream*: транспорт queue
// принимает только известные стримы (knownStream), псевдонимы потребовали
// бы правки прод-кода ради теста. Изоляция — выделенный номер БД Redis
// (redis.dev.conf: databases 16, сервисы приложения работают в db 0):
// тесты не видят чужих ключей и не мешают запущенному стеку, очистка
// ключей стримов идёт до и после каждой проверки внутри тестовой БД.
// PostgreSQL — изолированная схема с миграциями (migratedSchema), как
// тесты ingest.
package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
)

// resilienceRedisDB — выделенная БД Redis для тестов устойчивости:
// не db 0 работающих сервисов (dev-конфиг разрешает 16 БД).
const resilienceRedisDB = 15

// resilienceStreams — боевые стримы очереди (queue.go), целиком
// зачищаются между проверками внутри тестовой БД (см. комментарий файла).
var resilienceStreams = []string{
	queue.StreamDNS, queue.StreamHTTP, queue.StreamRender,
	queue.StreamAnalyze, queue.StreamDead,
}

// resilienceRedisEnv — клиент живого Redis testbed (выделенная БД) с
// полной очисткой стримов очереди до и после теста.
func resilienceRedisEnv(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: resilienceRedisDB})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("подключение к Redis %s: %v", redisAddr, err)
	}
	wipeQueueStreams(t, rdb)
	t.Cleanup(func() {
		wipeQueueStreams(t, rdb)
		_ = rdb.Close()
	})
	return rdb
}

// wipeQueueStreams удаляет ключи стримов очереди вместе с их группами.
func wipeQueueStreams(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Del(ctx, resilienceStreams...).Err(); err != nil {
		t.Errorf("очистка стримов %v: %v", resilienceStreams, err)
	}
}

// streamLen — XLEN: все записи стрима, включая доставленные и не
// подтверждённые XACK (их убирает только обрезка MAXLEN, T-025).
func streamLen(t *testing.T, rdb *redis.Client, stream string) int64 {
	t.Helper()
	n, err := rdb.XLen(context.Background(), stream).Result()
	if err != nil {
		t.Fatalf("XLEN %s: %v", stream, err)
	}
	return n
}

// pendingCount — число неподтверждённых записей группы. XPENDING
// EXTENDED, а не сводная форма: пустой PEL — пустой срез без nil-ответа,
// число не зависит от версии Redis.
func pendingCount(t *testing.T, rdb *redis.Client, stream, group string) int {
	t.Helper()
	res, err := rdb.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  1000,
	}).Result()
	if err != nil {
		t.Fatalf("XPENDING %s/%s: %v", stream, group, err)
	}
	return len(res)
}

// waitPendingIdle — детерминированное ожидание «состаривания» зависших
// записей группы: опрос XPENDING EXTENDED (чтение без побочных эффектов
// — счётчик доставок и потребитель не меняются), пока простой каждой из
// n записей не превысит minIdle. К моменту возврата XAUTOCLAIM с этим
// minIdle обязан вернуть записи. Слепой Sleep над MinIdle давал запас
// 10–20 мс и флакал на нагруженном раннере: съеденный планировщиком сон
// оставлял записи «недозревшими», Claim возвращал 0 — ложное падение.
func waitPendingIdle(t *testing.T, rdb *redis.Client, stream, group string, n int, minIdle time.Duration) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := rdb.XPendingExt(context.Background(), &redis.XPendingExtArgs{
			Stream: stream,
			Group:  group,
			Start:  "-",
			End:    "+",
			Count:  1000,
		}).Result()
		if err != nil {
			t.Fatalf("XPENDING %s/%s: %v", stream, group, err)
		}
		ready := 0
		for _, p := range res {
			if p.Idle > minIdle {
				ready++
			}
		}
		if ready >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("записи %s/%s не состарились до %s за 5с (готово %d из %d)", stream, group, minIdle, ready, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// entryIDs — идентификаторы всех записей стрима (XRANGE).
func entryIDs(t *testing.T, rdb *redis.Client, stream string) []string {
	t.Helper()
	msgs, err := rdb.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", stream, err)
	}
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	return ids
}

// consumeTasks — читает до n записей стрима от потребителя consumer
// (Block укорочен в RedisOptions теста: пустое чтение не висит секунды).
func consumeTasks(t *testing.T, q *queue.RedisQueue, stream, group, consumer string, n int) []queue.Delivery {
	t.Helper()
	ds, err := q.Consume(context.Background(), stream, group, consumer, n)
	if err != nil {
		t.Fatalf("Consume %s (группа %s, потребитель %s): %v", stream, group, consumer, err)
	}
	return ds
}

// insertPendingDomains — зона и pending-домены с метками labels прямыми
// вставками в изолированную тестовую схему (сквозной конвейер ingest —
// территория T-023); возвращает id доменов.
func insertPendingDomains(t *testing.T, pool *pgxpool.Pool, zone string, labels []string) []int64 {
	t.Helper()
	ctx := context.Background()
	var zoneID int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ($1) RETURNING id", zone,
	).Scan(&zoneID); err != nil {
		t.Fatalf("вставка зоны %s: %v", zone, err)
	}
	rows, err := pool.Query(ctx,
		"INSERT INTO domains (zone_id, label) SELECT $1, unnest($2::text[]) RETURNING id",
		zoneID, labels)
	if err != nil {
		t.Fatalf("вставка доменов зоны %s: %v", zone, err)
	}
	defer rows.Close()
	ids := make([]int64, 0, len(labels))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("чтение id домена: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("вставка доменов зоны %s: %v", zone, err)
	}
	return ids
}

// countDomains — число строк domains по условию where.
func countDomains(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	query := "SELECT count(*) FROM domains"
	if where != "" {
		query += " WHERE " + where
	}
	if err := pool.QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// stateWhere — условие countDomains по check_state.
func stateWhere(s model.CheckState) string {
	return fmt.Sprintf("check_state = %d", int16(s))
}

// idSet — множество для сравнения без порядка.
func idSet[T comparable](xs []T) map[T]bool {
	m := make(map[T]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// TestResilience_ProcessingAckAfterDB — «публикация → потребление →
// запись результата в БД → XACK» (ТЗ 4.2, 3.4; Processing, T-025):
// успешная обработка подтверждается только после записи в БД, сбой
// обработки записи не подтверждает; рестарт-подобный разрыв (новый
// потребитель группы читает только ">") не переотдаёт записи, а зависшую
// возвращает клейм — повторная обработка завершает цикл. Ни один домен
// не теряется: итоговое состояние БД покрывает все пачки.
func TestResilience_ProcessingAckAfterDB(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	pool := migratedSchema(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 30 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-dns"

	// 6 pending-доменов двумя пачками по 3 — как выбирает EnqueueLoop:
	// после перевода первой пачки в queued диспетчер отдаёт следующую.
	labels := []string{"h01", "h02", "h03", "h04", "h05", "h06"}
	if ids := insertPendingDomains(t, pool, "resil.test", labels); len(ids) != len(labels) {
		t.Fatalf("вставлено доменов: %d, хочу %d", len(ids), len(labels))
	}
	for i := 0; i < 2; i++ {
		batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 3})
		if err != nil {
			t.Fatalf("выборка пачки №%d: %v", i+1, err)
		}
		if len(batch) != 3 {
			t.Fatalf("пачка №%d: выбрано %d доменов, хочу 3", i+1, len(batch))
		}
		if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
			t.Fatalf("постановка пачки №%d: %v", i+1, err)
		}
	}

	// Оба сообщения опубликованы до создания группы: ленивое XGROUP
	// CREATE со стартом "0" доносит и предысторию — первый потребитель
	// получает обе пачки.
	got := consumeTasks(t, q, queue.StreamDNS, group, "w1", 10)
	if len(got) != 2 {
		t.Fatalf("потреблено сообщений: %d, хочу 2 (обе пачки, история до группы)", len(got))
	}

	// Запись результата идемпотентна (ТЗ 3.4): перевод queued → done
	// охраной текущего состояния — повторная доставка ничего не меняет.
	writeResult := func(ctx context.Context, d queue.Delivery) error {
		batch, err := queue.Decode[queue.DNSBatch](d)
		if err != nil {
			return err
		}
		domainIDs := make([]int64, len(batch.Domains))
		for i, dom := range batch.Domains {
			domainIDs[i] = dom.DomainID
		}
		_, err = pool.Exec(ctx, `
			UPDATE domains SET check_state = $1, last_checked_at = now()
			WHERE zone_id = $2 AND id = ANY($3) AND check_state = $4`,
			int16(model.CheckStateDone), batch.Domains[0].ZoneID, domainIDs,
			int16(model.CheckStateQueued))
		return err
	}

	// Первая пачка обработана: запись в БД → XACK.
	if err := queue.Processing(ctx, q, group, got[0], writeResult); err != nil {
		t.Fatalf("Processing первой пачки: %v", err)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateDone)); n != 3 {
		t.Fatalf("done после первой пачки: %d, хочу 3 (XACK только после записи в БД)", n)
	}

	// Вторая пачка: «воркер умер» до записи результата — Processing
	// возвращает ошибку, XACK нет, запись остаётся в pending группы.
	workerDied := errors.New("воркер пал до записи результата")
	if err := queue.Processing(ctx, q, group, got[1],
		func(context.Context, queue.Delivery) error { return workerDied }); err == nil {
		t.Fatal("Processing сбойной обработки вернул nil")
	}
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 1 {
		t.Fatalf("неподтверждённых записей: %d, хочу 1", n)
	}

	// Рестарт-подобный разрыв: новый потребитель читает только новые
	// записи (">") — ни подтверждённые, ни зависшие не переотдаются.
	if got := consumeTasks(t, q, queue.StreamDNS, group, "w2", 10); len(got) != 0 {
		t.Fatalf("новый потребитель получил %d записей, хочу 0", len(got))
	}

	// Клейм возвращает зависшую запись (attempts = 2: первая доставка +
	// клейм), повторная обработка завершает цикл.
	waitPendingIdle(t, rdb, queue.StreamDNS, group, 1, 30*time.Millisecond)
	claimed, _, err := reliab.Claim(ctx, queue.StreamDNS, group, "w2", 30*time.Millisecond, 10, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != got[1].ID {
		t.Fatalf("клейм вернул %+v, хочу запись %s", claimed, got[1].ID)
	}
	if claimed[0].Attempts != 2 {
		t.Errorf("попыток после клейма: %d, хочу 2 (доставка + клейм)", claimed[0].Attempts)
	}
	if err := queue.Processing(ctx, q, group, claimed[0].Delivery, writeResult); err != nil {
		t.Fatalf("Processing восстановленной пачки: %v", err)
	}

	// Данные не потеряны: все 6 доменов прошли проверку, pending пуст,
	// в q:dead ничего не улеглось.
	if n := countDomains(t, pool, stateWhere(model.CheckStateDone)); n != 6 {
		t.Errorf("done в итоге: %d, хочу 6", n)
	}
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("неподтверждённых записей в итоге: %d, хочу 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("записей в q:dead: %d, хочу 0", n)
	}
}

// TestResilience_WorkerDiesMidBatch — остановка воркера посреди пачки
// (ТЗ 11.2): потребитель получил записи (XREADGROUP), первые доработал
// и подтвердил, остальные — нет («умер»). Новый потребитель с ">" их не
// получает: записи возвращает XAUTOCLAIM (Reliab.Claim) с исходным телом;
// доработка восстанавливает все task_id, ни одна задача не теряется
// и не попадает в q:dead.
func TestResilience_WorkerDiesMidBatch(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 30 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-http"
	const total = 6

	// Пачка штучных задач q:http с известными task_id.
	wantTasks := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		task := fmt.Sprintf("t-mid-%02d", i+1)
		wantTasks[task] = true
		if _, err := q.Publish(ctx, queue.StreamHTTP, queue.HTTPTask{
			TaskID:     task,
			ZoneID:     7,
			DomainID:   int64(100 + i),
			Name:       fmt.Sprintf("mid%02d.resil.test", i+1),
			Subdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}},
		}); err != nil {
			t.Fatalf("публикация задачи %s: %v", task, err)
		}
	}

	batch := consumeTasks(t, q, queue.StreamHTTP, group, "w1", 10)
	if len(batch) != total {
		t.Fatalf("потреблено задач: %d, хочу %d", len(batch), total)
	}
	origPayload := make(map[string][]byte, total)
	for _, d := range batch {
		origPayload[d.ID] = d.Payload
	}

	// Первые две доработаны (запись в БД → XACK), на третьей воркер
	// умер: остаток пачки не подтверждён.
	if err := q.Ack(ctx, queue.StreamHTTP, group, batch[0].ID, batch[1].ID); err != nil {
		t.Fatalf("XACK доработанных: %v", err)
	}

	// Новый потребитель не получает чужие неподтверждённые записи.
	if got := consumeTasks(t, q, queue.StreamHTTP, group, "w2", 10); len(got) != 0 {
		t.Fatalf("новый потребитель получил %d чужих записей, хочу 0", len(got))
	}

	// Проход восстановления: XAUTOCLAIM возвращает остаток пачки
	// (2 из 6 подтверждены выше).
	waitPendingIdle(t, rdb, queue.StreamHTTP, group, total-2, 30*time.Millisecond)
	claimed, _, err := reliab.Claim(ctx, queue.StreamHTTP, group, "recover", 30*time.Millisecond, total, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != total-2 {
		t.Fatalf("клейм вернул %d записей, хочу %d (недоработанный остаток пачки)",
			len(claimed), total-2)
	}
	doneTasks := make(map[string]bool, total)
	for _, d := range batch[:2] { // доработанные до падения
		task, err := queue.Decode[queue.HTTPTask](d)
		if err != nil {
			t.Fatalf("декодирование записи %s: %v", d.ID, err)
		}
		doneTasks[task.TaskID] = true
	}
	for _, c := range claimed {
		if c.Attempts != 2 {
			t.Errorf("запись %s: попыток %d, хочу 2 (доставка + клейм)", c.ID, c.Attempts)
		}
		if string(c.Payload) != string(origPayload[c.ID]) {
			t.Errorf("запись %s: тело изменено клеймом:\n получил %s\n хочу   %s",
				c.ID, c.Payload, origPayload[c.ID])
		}
		task, err := queue.Decode[queue.HTTPTask](c.Delivery)
		if err != nil {
			t.Fatalf("декодирование записи %s: %v", c.ID, err)
		}
		doneTasks[task.TaskID] = true
		// Восстановитель дорабатывает: обработка → XACK.
		if err := queue.Processing(ctx, q, group, c.Delivery,
			func(context.Context, queue.Delivery) error { return nil }); err != nil {
			t.Fatalf("Processing восстановленной записи %s: %v", c.ID, err)
		}
	}

	// Все task_id пачки пережили падение: каждый обработан ровно один
	// раз, ни один не потерялся и не улетел в q:dead.
	if len(doneTasks) != total {
		t.Fatalf("уникальных task_id после восстановления: %d, хочу %d", len(doneTasks), total)
	}
	for task := range wantTasks {
		if !doneTasks[task] {
			t.Errorf("задача %s потеряна при восстановлении", task)
		}
	}
	if n := pendingCount(t, rdb, queue.StreamHTTP, group); n != 0 {
		t.Errorf("неподтверждённых записей в итоге: %d, хочу 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("записей в q:dead: %d, хочу 0", n)
	}
}

// TestResilience_DeadLetterAfterMaxAttempts — счётчик попыток и перевод
// в q:dead (ТЗ 4.2, T-025): запись, которую никто не подтверждает, при
// каждом клейме наращивает deliveries-счётчик PEL; попытки MaxAttempts
// (3 по умолчанию) — ещё в работу, следующая — Dead (task_id, исходный
// стрим/запись, attempts, идентификаторы предмета без payload — T-105)
// и XACK исходной: запись покидает pending. Все task_id переживают
// цикл — живы в q:dead.
func TestResilience_DeadLetterAfterMaxAttempts(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	// MaxAttempts — дефолт 3 (DefaultMaxAttempts); MinIdle маленький:
	// 5 минут DefaultClaimMinIdle ждать в тесте незачем.
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 20 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-render"
	const total = 3

	wantRes := make(map[string]queue.Result, total)
	for i := 0; i < total; i++ {
		res := queue.Result{
			TaskID:      fmt.Sprintf("t-dead-%02d", i+1),
			ZoneID:      9,
			DomainID:    int64(200 + i),
			SubdomainID: 1,
			Name:        fmt.Sprintf("dead%02d.resil.test", i+1),
		}
		wantRes[res.TaskID] = res
		if _, err := q.Publish(ctx, queue.StreamRender, res); err != nil {
			t.Fatalf("публикация %s: %v", res.TaskID, err)
		}
	}
	batch := consumeTasks(t, q, queue.StreamRender, group, "w1", 10)
	if len(batch) != total {
		t.Fatalf("потреблено задач: %d, хочу %d", len(batch), total)
	}

	// Воркер пал, не подтвердив ничего. «Восстановители» тоже падают:
	// каждый проход клейма возвращает записи в работу, счётчик растёт
	// (доставка + клейм №1 = 2, №2 = 3 — последняя разрешённая).
	for round, wantAttempts := range []int{2, 3} {
		// Клейм предыдущего раунда закрепил записи за «recoverителем»
		// и сбросил им простой — состаривание ждётся заново.
		waitPendingIdle(t, rdb, queue.StreamRender, group, total, 20*time.Millisecond)
		claimed, _, err := reliab.Claim(ctx, queue.StreamRender, group, "recover", 20*time.Millisecond, total, "0-0")
		if err != nil {
			t.Fatalf("клейм №%d: %v", round+1, err)
		}
		if len(claimed) != total {
			t.Fatalf("клейм №%d вернул %d записей, хочу %d (лимит ещё не исчерпан)",
				round+1, len(claimed), total)
		}
		for _, c := range claimed {
			if c.Attempts != wantAttempts {
				t.Errorf("клейм №%d, запись %s: попыток %d, хочу %d",
					round+1, c.ID, c.Attempts, wantAttempts)
			}
		}
		// Подтверждения нет — записи снова зависают.
	}

	// Клейм №3: счётчик 4 > MaxAttempts 3 — записи не возвращаются,
	// а переводятся в q:dead и подтверждаются в исходном стриме.
	waitPendingIdle(t, rdb, queue.StreamRender, group, total, 20*time.Millisecond)
	if claimed, _, err := reliab.Claim(ctx, queue.StreamRender, group, "recover", 20*time.Millisecond, total, "0-0"); err != nil {
		t.Fatalf("клейм №3: %v", err)
	} else if len(claimed) != 0 {
		t.Fatalf("клейм №3 вернул работу %+v, хочу перевод в q:dead", claimed)
	}
	if n := pendingCount(t, rdb, queue.StreamRender, group); n != 0 {
		t.Fatalf("исходные записи не покинули pending: %d, хочу 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != total {
		t.Fatalf("записей в q:dead: %d, хочу %d", n, total)
	}

	// Ручной разбор q:dead (T-024/T-025/T-105): формат Dead сохраняет
	// task_id, источник, attempts и идентификаторы предмета задачи —
	// ключ строки subdomain_results (зона/домен/субдомен/вариант/имя);
	// payload исходного сообщения записи не несут (байтовая защита:
	// пачки q:dns до ~250 КБ × потолок 10 000 ≈ 2,5 ГБ — см. T-105).
	deadMsgs := consumeTasks(t, q, queue.StreamDead, "resil-dead", "operator", 10)
	if len(deadMsgs) != total {
		t.Fatalf("прочитано из q:dead: %d, хочу %d", len(deadMsgs), total)
	}
	seen := make(map[string]bool, total)
	for _, d := range deadMsgs {
		dead, err := queue.Decode[queue.Dead](d)
		if err != nil {
			t.Fatalf("декодирование Dead: %v", err)
		}
		res, ok := wantRes[dead.TaskID]
		if !ok || seen[dead.TaskID] {
			t.Errorf("Dead.task_id = %q — вне исходной пачки или дубль", dead.TaskID)
			continue
		}
		seen[dead.TaskID] = true
		if dead.Stream != queue.StreamRender {
			t.Errorf("Dead.stream = %q, хочу %q", dead.Stream, queue.StreamRender)
		}
		if dead.ZoneID != res.ZoneID || dead.DomainID != res.DomainID ||
			dead.SubdomainID != res.SubdomainID || dead.Variant != res.Variant || dead.Name != res.Name {
			t.Errorf("Dead записи %s: идентификаторы %+v, хочу предмет задачи %+v",
				dead.EntryID, dead, res)
		}
		if len(dead.Payload) != 0 {
			t.Errorf("Dead.payload записи %s перенесён (T-105 — записи без payload): %d байт",
				dead.EntryID, len(dead.Payload))
		}
		if dead.Attempts != 4 {
			t.Errorf("Dead.attempts = %d, хочу 4 (доставка + 3 клейма)", dead.Attempts)
		}
	}
	if len(seen) != total {
		t.Errorf("уникальных task_id в q:dead: %d, хочу %d", len(seen), total)
	}
}

// TestResilience_RedisLossSchedulerRecovery — потеря Redis (ТЗ 3.4,
// 11.4 п. 9): задачи исчезают вместе со стримами и группами, домены
// «зависают» в queued. Возврат истёкших lease (T-029) возвращает их
// в pending, диспетчер (T-027) перевыбирает, постановка (T-028)
// переиздаёт пачку; живой воркер пересоздаёт группу после NOGROUP и
// потребляет переизданное. Источник истины PostgreSQL: счёт и
// id-множество доменов до/после совпадают.
func TestResilience_RedisLossSchedulerRecovery(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	pool := migratedSchema(t)
	// Один экземпляр очереди на весь сценарий — как живой процесс
	// воркера, переживший потерю Redis (кэш созданных групп устаревает).
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-dns"

	labels := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		labels = append(labels, fmt.Sprintf("d%03d", i))
	}
	wantIDs := idSet(insertPendingDomains(t, pool, "resil.test", labels))
	if len(wantIDs) != 10 {
		t.Fatalf("вставлено доменов: %d, хочу 10", len(wantIDs))
	}

	// Цикл планировщика: выборка → перевод в queued с lease → пачка
	// в q:dns (T-027/T-028).
	batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("выборка пачки: %v", err)
	}
	if len(batch) != 10 {
		t.Fatalf("выбрано доменов: %d, хочу 10", len(batch))
	}
	res, err := scheduler.EnqueueBatch(ctx, pool, q, batch)
	if err != nil {
		t.Fatalf("постановка пачки: %v", err)
	}
	if res.Queued != 10 || res.Published != 10 {
		t.Fatalf("постановка: queued %d, published %d; хочу 10/10", res.Queued, res.Published)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != 10 {
		t.Fatalf("queued после постановки: %d, хочу 10", n)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 1 {
		t.Fatalf("записей в q:dns: %d, хочу 1 (одна пачка)", n)
	}

	// Воркер взял задачу (группа создана), подтверждения не было.
	if got := consumeTasks(t, q, queue.StreamDNS, group, "w1", 5); len(got) != 1 {
		t.Fatalf("воркер получил задач: %d, хочу 1", len(got))
	}

	// Потеря Redis: ключи стримов (и группы в них) уничтожены — как
	// flush утерянного инстанса. DEL только ключей теста: чужие данные
	// не трогаются (полная очистка — resilienceRedisEnv).
	if err := rdb.Del(ctx, resilienceStreams...).Err(); err != nil {
		t.Fatalf("удаление стримов (потеря Redis): %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 0 {
		t.Fatalf("после потери Redis записей в q:dns: %d, хочу 0", n)
	}

	// Домены «зависли» в queued — задачи больше нет, результата нет.
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != 10 {
		t.Fatalf("домены не зависли в queued: %d, хочу 10", n)
	}

	// Lease-возврат (T-029): время до истечения сокращено прямым
	// обновлением queued_at в прошлое (детерминированно, без ожидания
	// scheduler.lease 30 минут) — час назад заведомо старше порога при
	// любых расхождениях часов клиента и сервера БД.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET queued_at = now() - interval '1 hour'"); err != nil {
		t.Fatalf("старение queued_at: %v", err)
	}
	lease, err := scheduler.LeaseOnce(ctx, pool, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if lease.Domains != 10 {
		t.Fatalf("возврат по lease: %d доменов, хочу 10", lease.Domains)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStatePending)); n != 10 {
		t.Fatalf("pending после возврата lease: %d, хочу 10", n)
	}
	if n := countDomains(t, pool, "queued_at IS NOT NULL"); n != 0 {
		t.Errorf("queued_at не сброшен у возвращённых: %d, хочу 0", n)
	}

	// Полный цикл планировщика после потери: перевыбор и переиздание.
	batch2, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("перевыборка пачки: %v", err)
	}
	if len(batch2) != 10 {
		t.Fatalf("перевыбрано доменов: %d, хочу 10", len(batch2))
	}
	res2, err := scheduler.EnqueueBatch(ctx, pool, q, batch2)
	if err != nil {
		t.Fatalf("переиздание пачки: %v", err)
	}
	if res2.Queued != 10 || res2.Published != 10 {
		t.Fatalf("переиздание: queued %d, published %d; хочу 10/10", res2.Queued, res2.Published)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 1 {
		t.Fatalf("записей в q:dns после переиздания: %d, хочу 1", n)
	}

	// Живой воркер (тот же процесс) читает переизданное: группа
	// пересоздаётся после NOGROUP — рестарт процесса не нужен.
	msgs := consumeTasks(t, q, queue.StreamDNS, group, "w1", 5)
	if len(msgs) != 1 {
		t.Fatalf("воркер получил задач после потери: %d, хочу 1", len(msgs))
	}
	dnsBatch, err := queue.Decode[queue.DNSBatch](msgs[0])
	if err != nil {
		t.Fatalf("декодирование переизданной пачки: %v", err)
	}
	gotIDs := make(map[int64]bool, len(dnsBatch.Domains))
	for _, dom := range dnsBatch.Domains {
		gotIDs[dom.DomainID] = true
		if dom.Name == "" {
			t.Errorf("домен %d в переизданной пачке без имени", dom.DomainID)
		}
	}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("в переизданной пачке доменов: %d, хочу %d", len(gotIDs), len(wantIDs))
	}
	for id := range wantIDs {
		if !gotIDs[id] {
			t.Errorf("домен %d потерян при восстановлении", id)
		}
	}

	// Данные не потерялись: счёт доменов не изменился, все снова
	// в работе (queued).
	if n := countDomains(t, pool, ""); n != 10 {
		t.Errorf("доменов в БД: %d, хочу 10", n)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != 10 {
		t.Errorf("queued после переиздания: %d, хочу 10", n)
	}
}

// TestResilience_BackpressureGate — гейт ёмкости (T-026; ТЗ 4.2/5.3
// «строго ниже порога»): ниже порога публикация проходит, на пороге —
// блокируется до освобождения ёмкости (XACK длину не снижает — записи
// покидают стрим только обрезкой/XDEL), отмена контекста в ожидании —
// чистый выход ошибкой контекста (graceful shutdown, ТЗ 3.4).
func TestResilience_BackpressureGate(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	const watermark = 5
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(watermark),
		Interval:  50 * time.Millisecond,
	})
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	pub := queue.NewWatermarkPublisher(q, gate)
	ctx := context.Background()

	task := func(i int) queue.Result {
		return queue.Result{
			TaskID:      fmt.Sprintf("t-bp-%03d", i),
			ZoneID:      3,
			DomainID:    int64(i),
			SubdomainID: 0,
		}
	}

	// Ниже порога — публикации проходят; каждая проверка идёт до
	// добавления записи, поэтому пятая публикация проходит по длине 4
	// и доводит стрим ровно до порога.
	for i := 1; i <= watermark; i++ {
		if _, err := pub.Publish(ctx, queue.StreamRender, task(i)); err != nil {
			t.Fatalf("публикация №%d до порога: %v", i, err)
		}
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != watermark {
		t.Fatalf("длина стрима: %d, хочу %d (порог)", n, watermark)
	}

	// Порог достигнут (len == watermark): следующая публикация
	// блокируется — «строго ниже порога».
	blocked := make(chan error, 1)
	go func() {
		_, err := pub.Publish(ctx, queue.StreamRender, task(watermark+1))
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("публикация на пороге прошла без ожидания: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// Ёмкость освободилась (записи покинули стрим) — публикация
	// проходит.
	if err := rdb.XDel(ctx, queue.StreamRender,
		entryIDs(t, rdb, queue.StreamRender)...).Err(); err != nil {
		t.Fatalf("XDEL: %v", err)
	}
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("публикация после освобождения ёмкости: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("публикация не разблокировалась после освобождения ёмкости")
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
		t.Errorf("длина после публикации: %d, хочу 1", n)
	}

	// Отмена во время ожидания — чистый выход (graceful shutdown
	// продюсера не висит в гейте). Снова заполняем до порога.
	for i := 1; i < watermark; i++ {
		if _, err := q.Publish(ctx, queue.StreamRender, task(100+i)); err != nil {
			t.Fatalf("заполнение до порога: %v", err)
		}
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	shutdown := make(chan error, 1)
	go func() {
		_, err := pub.Publish(cancelCtx, queue.StreamRender, task(999))
		shutdown <- err
	}()
	select {
	case err := <-shutdown:
		t.Fatalf("публикация прошла до отмены: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-shutdown:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("отмена ожидания гейта: %v, хочу context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("отмена контекста не прервала ожидание гейта")
	}
}

// TestResilience_MaxLenTrims — MAXLEN ~ (ТЗ 4.2, T-025): публикация
// сверх предела обрезает стрим — длина держится у порога и не растёт с
// числом публикаций, старые записи уходят (первой опубликованной записи
// в стриме нет).
func TestResilience_MaxLenTrims(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	const maxLen = 40
	// Явный предел вместо политики стрима: проверяется сама обрезка
	// XADD MAXLEN ~ на малых числах, не мегазаписи q:analyze.
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{MaxLen: maxLen, Block: 150 * time.Millisecond})
	ctx := context.Background()

	const published = 300
	first := ""
	for i := 0; i < published; i++ {
		id, err := q.Publish(ctx, queue.StreamAnalyze, queue.Result{
			TaskID:      fmt.Sprintf("t-ml-%04d", i+1),
			ZoneID:      2,
			DomainID:    int64(i + 1),
			SubdomainID: 0,
		})
		if err != nil {
			t.Fatalf("публикация №%d: %v", i+1, err)
		}
		if first == "" {
			first = id
		}
	}

	// MAXLEN ~ обрезает с точностью до узла списка записей: длина
	// около порога, но заведомо ограничена — не растёт с публикациями.
	// Перелёт до одного макроузла listpack (~30–40 таких сообщений
	// в узле 4 КБ; на живом стенде после 300 публикаций длина 57),
	// потолок maxLen*2 отсекает «обрезки нет» (там было бы 300)
	// с запасом на размер узла.
	n := streamLen(t, rdb, queue.StreamAnalyze)
	if n < 1 || n > maxLen*2 {
		t.Fatalf("длина q:analyze после %d публикаций: %d, хочу 1..%d (MAXLEN ~ %d, запас — один макроузел списка записей)",
			published, n, maxLen*2, maxLen)
	}

	// Старые записи усечены: самая ранняя запись стрима новее первой
	// опубликованной.
	oldest, err := rdb.XRangeN(ctx, queue.StreamAnalyze, "-", "+", 1).Result()
	if err != nil || len(oldest) != 1 {
		t.Fatalf("XRANGE q:analyze: %d записей, err=%v", len(oldest), err)
	}
	if oldest[0].ID <= first {
		t.Errorf("самая ранняя запись %s не новее первой опубликованной %s — обрезки не было",
			oldest[0].ID, first)
	}
}

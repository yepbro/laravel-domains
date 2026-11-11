// Тесты триажа выживших мутантов CI-прогона 10.11.2026 (часть B,
// score 0.857): добивки покрытия queue по группам разбора — точные
// значения констант и границ withDefaults (литеральные пины, сравнение
// с самой константой мутанта не замечает), ошибки шагов проходов
// (глушение возвратов, счёт при ошибке обязан быть нулём), разрывы
// циклов (break вместо continue), границы порогов чистки консьюмеров,
// текстовые ошибки Redis («no such key», префиксы ответов сервера на
// голых строках — строковый fallback Is*-хелперов), остановка повторов
// XADD после успеха/не-инфраструктурной ошибки, аргументы
// перепроверки PEL (окно гонки сжато до одной записи), клампы и
// защитные ветки возврата попыток, границы точки обрезки (гонка
// «XINFO→XPENDING», ровно 3/4 потолка), сигналы застрявшего префикса,
// нулевой счёт тарпит-счётчика на ошибках валидации/команд и выход
// RecoverLoop без клейма после отмены ctx. Логирование и эквивалентные
// мутанты — вне тестов (блэклист, docs/mutation-testing.md §4).
package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- consumers.go ----------------------------------------------------------

// triConsumersFake — fakeConsumers с заготовленными ошибками DELCONSUMER
// по кандидату «stream|group|consumer» (базовый фейк отвечает успехом
// всегда): глушение возврата ошибки удаления и разрыв цикла по кандидату.
type triConsumersFake struct {
	*fakeConsumers
	delErrs map[string]error
}

func (f *triConsumersFake) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	f.fakeConsumers.XGroupDelConsumer(ctx, stream, group, consumer) // запись протокола
	if err, ok := f.delErrs[stream+"|"+group+"|"+consumer]; ok {
		return redis.NewIntResult(0, err)
	}
	return redis.NewIntResult(f.delPEL, nil)
}

// TestConsumersTriagePins — литеральные пины констант гигиены и границы
// withDefaults: порог DefaultConsumerIdle — ровно 2 ч; единичные
// (1 нс) Idle/Interval — законные явные значения, дефолт не заменяет их.
func TestConsumersTriagePins(t *testing.T) {
	if DefaultConsumerIdle != 2*time.Hour {
		t.Errorf("DefaultConsumerIdle = %s, хочу ровно 2ч (T-155: суровые рестарты волн)", DefaultConsumerIdle)
	}
	o := ConsumerOptions{Idle: time.Nanosecond, Interval: time.Nanosecond}.withDefaults()
	if o.Idle != time.Nanosecond || o.Interval != time.Nanosecond {
		t.Errorf("единичные Idle/Interval заменены дефолтами: %+v (дефолт — только для ≤ 0)", o)
	}
}

// TestConsumersTriageIdleBoundary — неактивность ровно в пороге:
// консьюмер с idle == Idle — кандидат (порог «неактивен ≥»), удаляется.
func TestConsumersTriageIdleBoundary(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 2 * time.Hour}, // ровно порог
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (idle == порог — уже мёртвый)", removed)
	}
}

// TestConsumersTriageXInfoErrors — ошибка чтения списка консьюмеров
// группы: нет стрима/группы (все три формы — redis.Nil, текст «no such
// key», NOGROUP) — пустой проход без ошибки; прочая ошибка — ошибка
// прохода (глушение возврата молчало бы: счёт 0, DELCONSUMER нет).
func TestConsumersTriageXInfoErrors(t *testing.T) {
	quiet := map[string]error{
		"redis.Nil":         redis.Nil,
		"текст no such key": errors.New("ERR no such key"), // текстовая форма ответа Redis
		"NOGROUP":           errors.New("NOGROUP No such consumer group 'x' for key name 'y'"),
	}
	for name, err := range quiet {
		f := &fakeConsumers{listsErr: map[string]error{StreamHTTP + "|http-worker": err}}
		removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
			Groups: map[string]string{StreamHTTP: "http-worker"},
		})
		if err != nil {
			t.Errorf("%s: %v (нет стрима/группы — не ошибка)", name, err)
		}
		if removed != 0 {
			t.Errorf("%s: удалено %d, хочу 0", name, removed)
		}
	}

	f := &fakeConsumers{listsErr: map[string]error{StreamDNS + "|dns-worker": errors.New("connection reset")}}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
	})
	if err == nil {
		t.Fatal("ошибка чтения списка консьюмеров проглочена")
	}
	if removed != 0 {
		t.Errorf("удалено %d при ошибке, хочу 0", removed)
	}
}

// TestConsumersTriageNilClientCounts — nil-клиент: ошибка с НУЛЕВЫМ
// счётом удаления (Once) и немедленная ошибка цикла (Loop).
func TestConsumersTriageNilClientCounts(t *testing.T) {
	removed, err := CleanupConsumersOnce(context.Background(), nil, ConsumerOptions{})
	if err == nil {
		t.Fatal("nil-клиент не отвергнут")
	}
	if removed != 0 {
		t.Errorf("удалено %d при nil-клиенте, хочу 0", removed)
	}
	loopErr := make(chan error, 1)
	go func() { loopErr <- CleanupConsumersLoop(context.Background(), nil, ConsumerOptions{}) }()
	select {
	case err := <-loopErr:
		if err == nil {
			t.Error("цикл с nil-клиентом не вернул ошибку")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CleanupConsumersLoop с nil-клиентом не завершился ошибкой")
	}
}

// TestConsumersTriageStreamLoopBreaks — ошибка пары (неизвестный стрим,
// пустая группа) и сбой кандидата (перепроверка pending, гонка
// оживления, сбой DELCONSUMER) не разрывают проход: группы после
// сбойной чистятся, второй кандидат сбойной группы тоже обрабатывается.
func TestConsumersTriageStreamLoopBreaks(t *testing.T) {
	// Неизвестный стрим сортируется РАНЬШЕ рабочего: break остановил бы
	// проход до рабочей группы.
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{"q:aaa": "g", StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("неизвестный стрим не возвращён ошибкой")
	}
	if removed != 1 || len(f.delCalls()) != 1 {
		t.Errorf("после сбойной пары: удалено %d, DELCONSUMER %v — рабочий стрим обязан чиститься", removed, f.delCalls())
	}

	// Пустая группа той же карты — тоже не разрывает обход.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err = CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{"q:aaa": "", StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("пустая группа не возвращена ошибкой")
	}
	if removed != 1 {
		t.Errorf("после пустой группы: удалено %d, хочу 1", removed)
	}

	// Внутри группы: сбой перепроверки у первого кандидата — второй всё
	// равно удаляется; ошибка первого возвращается.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour},
				{Name: "http-worker-2", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		pendErr: map[string]error{StreamHTTP + "|http-worker|http-worker-1": errors.New("LOADING")},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err = CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("сбой перепроверки pending не возвращён ошибкой")
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (второй кандидат после сбойной перепроверки)", removed)
	}

	// Гонка оживления у первого (перепроверка увидела pending) — второй
	// кандидат удаляется.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour},
				{Name: "http-worker-2", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		pendRes: map[string][]redis.XPendingExt{
			StreamHTTP + "|http-worker|http-worker-1": {{ID: "5-0"}},
		},
	}
	removed, err = CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   time.Hour,
	})
	if err != nil {
		t.Fatalf("гонка оживления — не ошибка: %v", err)
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (после гонки оживления первого)", removed)
	}

	// Сбой DELCONSUMER первого — ошибка с нулевым счётом этой попытки;
	// второй кандидат удаляется (continue, не break).
	tf := &triConsumersFake{
		fakeConsumers: &fakeConsumers{
			lists: map[string][]redis.XInfoConsumer{
				StreamDNS + "|dns-worker": {
					{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour},
					{Name: "dns-worker-2", Pending: 0, Idle: 3 * time.Hour},
				},
			},
			pendRes: map[string][]redis.XPendingExt{},
		},
		delErrs: map[string]error{
			StreamDNS + "|dns-worker|dns-worker-1": errors.New("OOM command not allowed"),
		},
	}
	removed, err = CleanupConsumersOnce(context.Background(), tf, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("сбой DELCONSUMER не возвращён ошибкой")
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (первый сбой — не удалён, второй удалён)", removed)
	}
	dels := tf.delCalls()
	if len(dels) != 2 || dels[1] != StreamDNS+"|dns-worker|dns-worker-2" {
		t.Errorf("DELCONSUMER %v, хочу обе попытки", dels)
	}

	// Одиночный кандидат со сбоем DELCONSUMER: ошибка, счёт строго 0
	// (инкремент — только после успеха).
	tf = &triConsumersFake{
		fakeConsumers: &fakeConsumers{
			lists: map[string][]redis.XInfoConsumer{
				StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
			},
			pendRes: map[string][]redis.XPendingExt{},
		},
		delErrs: map[string]error{
			StreamDNS + "|dns-worker|dns-worker-1": errors.New("OOM command not allowed"),
		},
	}
	removed, err = CleanupConsumersOnce(context.Background(), tf, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil || removed != 0 {
		t.Errorf("сбой DELCONSUMER: (%d, %v), хочу (0, ошибка)", removed, err)
	}
}

// TestConsumersTriageEmptyGroupKnownStream — пустая группа ИЗВЕСТНОГО
// стрима (ветка group == "" — известность проверяется раньше): пара
// пропускается с ошибкой, обход продолжается (break рвал бы хвост —
// кейс «q:aaa» выше закрыл только ветку неизвестного стрима).
func TestConsumersTriageEmptyGroupKnownStream(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		// q:dns (пустая группа) сортируется раньше q:http: break
		// остановил бы проход до рабочей пары.
		Groups: map[string]string{StreamDNS: "", StreamHTTP: "http-worker"},
		Idle:   time.Hour,
	})
	if err == nil || !strings.Contains(err.Error(), StreamDNS) {
		t.Fatalf("пустая группа q:dns: %v, want ошибку про q:dns", err)
	}
	if removed != 1 || len(f.delCalls()) != 1 {
		t.Errorf("хвост за пустой группой: удалено %d, DELCONSUMER %v — хочу 1/1 (continue, не break)", removed, f.delCalls())
	}
}

// TestConsumersTriageSortedStreamOrder — пары обходятся по возрастанию
// имён стримов (детерминированный порядок ошибок и логов прохода):
// карта Go итерируется случайно, без сортировки порядок прыгал бы.
// Проход повторяется: случайный порядок хоть раз даёт сбой
// (вероятность промаха (1/6)^число повторов).
func TestConsumersTriageSortedStreamOrder(t *testing.T) {
	groups := map[string]string{StreamHTTP: "", StreamDNS: "", StreamAnalyze: ""}
	for i := 0; i < 50; i++ {
		_, err := CleanupConsumersOnce(context.Background(), &fakeConsumers{}, ConsumerOptions{Groups: groups})
		if err == nil {
			t.Fatal("три пустые группы: хочу ошибку прохода")
		}
		msg := err.Error()
		ia, id, ih := strings.Index(msg, StreamAnalyze), strings.Index(msg, StreamDNS), strings.Index(msg, StreamHTTP)
		if ia < 0 || id < 0 || ih < 0 || ia >= id || id >= ih {
			t.Fatalf("порядок ошибок не по возрастанию стримов: %q", msg)
		}
	}
}

// TestConsumersTriagePendingWindow — перепроверка PEL кандидата: окно
// гонки сжато до ОДНОЙ записи (Count 1, диапазон «-»…«+», ровно этот
// консьюмер); унесённый DELCONSUMER-ом pending в одну запись — уже
// гонка (ошибка оператору).
func TestConsumersTriagePendingWindow(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	}); err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if len(f.pendArgs) != 1 {
		t.Fatalf("перепроверок %d, хочу 1", len(f.pendArgs))
	}
	a := f.pendArgs[0]
	if a.Count != 1 || a.Start != "-" || a.End != "+" {
		t.Errorf("XPENDING EXTENDED %+v: хочу Count=1 Start=- End=+ (минимальное окно гонки)", a)
	}

	// Одна унесённая PEL-запись — уже недоставка навсегда: ошибка.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
		delPEL:  1,
	}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	}); err == nil {
		t.Error("DELCONSUMER унёс 1 pending-запись — хочу ошибку (грань > 0, не > 1)")
	}
}

// --- infra.go --------------------------------------------------------------

// TestInfraCauseTriagePlainStrings — ответы сервера на ГОЛЫХ строках:
// Is*-хелперы go-redis имеют строковый fallback, а обход префиксов в
// infraCause требует интерфейса redis.Error — классификация голых строк
// держится ровно дизъюнктами Is*-цепочки (их удаление молчало бы).
func TestInfraCauseTriagePlainStrings(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"OOM command not allowed when used memory > 'maxmemory'", true},
		{"LOADING Redis is loading the dataset in memory", true},
		{"READONLY You can't write against a read only replica", true},
		{"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'", true},
		{"CLUSTERDOWN The cluster is down", true},
		{"TRYAGAIN Multiple keys request during rehashing of slot", true},
		{"ERR wrong number of arguments", false},
	}
	for _, c := range cases {
		if got := infraCause(errors.New(c.text)); got != c.want {
			t.Errorf("infraCause(%q) = %v, хочу %v", c.text, got, c.want)
		}
	}
	// Транспорт в обёртке: неожиданный EOF и закрытый клиент.
	if !infraCause(fmt.Errorf("queue: publish: %w", io.ErrUnexpectedEOF)) {
		t.Error("io.ErrUnexpectedEOF в обёртке — инфраструктурный сбой")
	}
	if !infraCause(fmt.Errorf("queue: publish: %w", redis.ErrClosed)) {
		t.Error("redis.ErrClosed в обёртке — инфраструктурный сбой")
	}
}

// --- redisq.go -------------------------------------------------------------

// TestPublishRetryTriageStops — повторы XADD: успех раньше исчерпания
// повторов останавливает серию (лишних XADD нет); не-инфраструктурная
// ошибка в середине серии — останов без дожигания попыток.
func TestPublishRetryTriageStops(t *testing.T) {
	msg := DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}

	// Первый XADD — OOM, второй проходит: серия закончена, третий вызов
	// не нужен (останов по успеху, а не по исчерпанию).
	fake := &fakeRedis{addErrs: []error{oomErr}}
	q := NewRedisQueue(fake, RedisOptions{PublishRetries: 2, PublishRetryPause: time.Nanosecond})
	if _, err := q.Publish(context.Background(), StreamDNS, msg); err != nil {
		t.Fatalf("Publish после повтора: %v", err)
	}
	if len(fake.adds) != 2 {
		t.Errorf("вызовов XADD %d, хочу 2 (успех второго — серия остановлена)", len(fake.adds))
	}

	// OOM, затем не-инфраструктурная ошибка: повтор бессмыслен — серия
	// остановлена на ней, итог — та же ошибка без маркера InfraError.
	fake = &fakeRedis{addErrs: []error{oomErr, errors.New("ERR wrong number of arguments")}}
	q = NewRedisQueue(fake, RedisOptions{PublishRetries: 2, PublishRetryPause: time.Nanosecond})
	_, err := q.Publish(context.Background(), StreamDNS, msg)
	if err == nil || IsInfra(err) {
		t.Fatalf("не-инфраструктурная ошибка в середине серии: %v", err)
	}
	if len(fake.adds) != 2 {
		t.Errorf("вызовов XADD %d, хочу 2 (не-инфра ошибка — без дожигания попыток)", len(fake.adds))
	}
}

// --- reliab.go -------------------------------------------------------------

// TestReliabTriagePins — литеральные пины констант: TTL пробы записи (с
// запасом больше максимума redis.recover_interval 1 ч) и бюджетные
// оценки байта на запись (замер MEMORY USAGE T-141/T-092 — основание
// потолков DefaultStreamMaxLen, docs/redis.md §3.1).
func TestReliabTriagePins(t *testing.T) {
	if writeProbeTTL != 2*time.Hour {
		t.Errorf("writeProbeTTL = %s, хочу ровно 2ч", writeProbeTTL)
	}
	if DNSStreamBytesPerDomain != 66 {
		t.Errorf("DNSStreamBytesPerDomain = %d, хочу 66 (замер T-141)", DNSStreamBytesPerDomain)
	}
	if UnitStreamBytesWorst != 717 {
		t.Errorf("UnitStreamBytesWorst = %d, хочу 717 (верх замера T-092)", UnitStreamBytesWorst)
	}
	if DeadStreamBytesWorst != 205 {
		t.Errorf("DeadStreamBytesWorst = %d, хочу 205 (~150–300 Б T-105)", DeadStreamBytesWorst)
	}
}

// TestReliabTriageRefundArgs — защита аргументов возврата попыток:
// пустая группа/потребитель при непустой пачке — ошибка ДО Redis;
// пустая пачка — прозрачный nil независимо от группы; Refund без окна
// Do и RefundUnclassified без окна — прозрачный nil без команд.
func TestReliabTriageRefundArgs(t *testing.T) {
	claimed := []Claimed{{Delivery{StreamHTTP, "1-1", []byte(`{"task_id":"t1"}`)}, 2}}

	fake := &fakeClaimRedis{}
	r := newTestReliab(fake, ReliabOptions{})

	if err := r.Refund(context.Background(), "", "recover-1", claimed); err == nil || len(fake.dos) != 0 {
		t.Errorf("Refund с пустой группой: (%v, команд %d), хочу ошибку без команд", err, len(fake.dos))
	}
	if err := r.Refund(context.Background(), "http", "", claimed); err == nil || len(fake.dos) != 0 {
		t.Errorf("Refund с пустым потребителем: (%v, команд %d), хочу ошибку без команд", err, len(fake.dos))
	}
	if err := r.Refund(context.Background(), "", "c", nil); err != nil {
		t.Errorf("Refund пустой пачки с пустой группой: %v, хочу прозрачный nil", err)
	}
	if err := newTestReliab(&fakeClaimRedis{}, ReliabOptions{}).Refund(context.Background(), "", "c", nil); err != nil {
		t.Errorf("Refund(nil-пачка): %v, хочу nil", err)
	}

	// RefundUnclassified: те же защиты.
	ds := []Delivery{{Stream: StreamHTTP, ID: "1-1", Payload: []byte(`{"task_id":"t1"}`)}}
	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{})
	if err := r.RefundUnclassified(context.Background(), "", "recover-1", ds); err == nil || len(fake.dos) != 0 {
		t.Errorf("RefundUnclassified с пустой группой: (%v, команд %d), хочу ошибку без команд", err, len(fake.dos))
	}
	if err := r.RefundUnclassified(context.Background(), "http", "", ds); err == nil || len(fake.pends) != 0 {
		t.Errorf("RefundUnclassified с пустым потребителем: (%v, XPENDING %d), хочу ошибку до чтений", err, len(fake.pends))
	}
	if err := r.RefundUnclassified(context.Background(), "", "c", nil); err != nil {
		t.Errorf("RefundUnclassified пустой пачки: %v, хочу прозрачный nil", err)
	}

	// Клиент без окна Do: оба возврата прозрачны, попытки расходованы
	// как до T-222 — ни команд, ни чтений.
	claimOnly := struct{ claimStreams }{&fakeClaimRedis{}}
	ro := NewReliab(claimOnly, NewRedisQueue(claimOnly.claimStreams.(*fakeClaimRedis), RedisOptions{}), ReliabOptions{})
	emptyFake := claimOnly.claimStreams.(*fakeClaimRedis)
	if err := ro.Refund(context.Background(), "g", "c", claimed); err != nil {
		t.Errorf("Refund без окна Do: %v, хочу nil", err)
	}
	if err := ro.RefundUnclassified(context.Background(), "g", "c", ds); err != nil {
		t.Errorf("RefundUnclassified без окна Do: %v, хочу nil", err)
	}
	if len(emptyFake.pends) != 0 || len(emptyFake.dos) != 0 {
		t.Errorf("команды без окна Do: XPENDING %d, Do %d — гейт обязан молчать", len(emptyFake.pends), len(emptyFake.dos))
	}
}

// TestReliabTriageUnclassifiedContinue — классификация не обрывается на
// первой записи: сбой чтения счётчика или покинувшая PEL запись
// пропускаются, остальные записи получают возврат попытки.
func TestReliabTriageUnclassifiedContinue(t *testing.T) {
	// Первый — сбой чтения счётчика: ошибка в возврате, второй всё
	// равно возвращён (continue, не break).
	fake := &fakeClaimRedis{
		pendOneErr: map[string]error{"1-1": errors.New("connection reset")},
		pendRes:    map[string][]redis.XPendingExt{"1-2": {{ID: "1-2", RetryCount: 2}}},
	}
	ds := []Delivery{
		{Stream: StreamHTTP, ID: "1-1", Payload: []byte(`{"task_id":"t1"}`)},
		{Stream: StreamHTTP, ID: "1-2", Payload: []byte(`{"task_id":"t2"}`)},
	}
	err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(context.Background(), "http", "recover-1", ds)
	if err == nil || !contains(err, "счётчик доставок") {
		t.Fatalf("ошибка классификации: %v, хочу контекст счётчика", err)
	}
	if len(fake.dos) != 1 || fake.dos[0][5] != "1-2" {
		t.Errorf("возврат %v, хочу команду только для 1-2", fake.dos)
	}

	// Первая запись покинула PEL (пустой ответ): возврата не требует,
	// вторая возвращена.
	fake = &fakeClaimRedis{
		pendRes: map[string][]redis.XPendingExt{"1-2": {{ID: "1-2", RetryCount: 2}}},
	}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(context.Background(), "http", "recover-1", ds); err != nil {
		t.Fatalf("RefundUnclassified: %v", err)
	}
	if len(fake.dos) != 1 || fake.dos[0][5] != "1-2" {
		t.Errorf("возврат %v, хочу команду только для 1-2 (1-1 вне PEL)", fake.dos)
	}
}

// --- trim.go ---------------------------------------------------------------

// triTrimFake — fakeTrim с ошибками XTRIM/XLEN и подсчётом вызовов XLEN
// (базовый фейк не умеет ни того, ни другого): глушение ошибок команд и
// лишние чтения длины.
type triTrimFake struct {
	*fakeTrim
	trimErr   error
	xlenErr   error
	xlenCalls int
}

func (f *triTrimFake) XTrimMinID(ctx context.Context, key, minID string) *redis.IntCmd {
	f.fakeTrim.XTrimMinID(ctx, key, minID)
	if f.trimErr != nil {
		cmd := redis.NewIntCmd(ctx, "xtrim")
		cmd.SetErr(f.trimErr)
		return cmd
	}
	return redis.NewIntResult(f.trimCount, nil)
}

func (f *triTrimFake) XLen(ctx context.Context, key string) *redis.IntCmd {
	f.xlenCalls++
	if f.xlenErr != nil {
		cmd := redis.NewIntCmd(ctx, "xlen")
		cmd.SetErr(f.xlenErr)
		return cmd
	}
	return redis.NewIntResult(f.xlen[key], nil)
}

// triTrimStreamOnce — TrimOnce по одному стриму (сокращение).
func triTrimStreamOnce(t *testing.T, rdb trimStreams, opts TrimOptions) (int64, []string, error) {
	t.Helper()
	o := opts
	o.Streams = []string{StreamDNS}
	return TrimOnce(context.Background(), rdb, o)
}

// TestTrimTriageErrors — ошибки шагов прохода: чтение групп (кроме
// «нет ключа»), pending, XTRIM, XLEN — ошибки прохода с нулевым (или
// точным для XLEN) счётом; после сбоя XTRIM длина не читается.
func TestTrimTriageErrors(t *testing.T) {
	// Прочая ошибка XINFO GROUPS — ошибка (нет ключа — нет, см. ниже).
	f := &triTrimFake{fakeTrim: &fakeTrim{
		groupsErr: map[string]error{StreamDNS: errors.New("connection reset")},
	}}
	n, _, err := triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("ошибка чтения групп проглочена")
	}
	if n != 0 {
		t.Errorf("удалено %d при ошибке, хочу 0", n)
	}

	// Ошибка XPENDING группы — стрим пропущен с ошибкой, счёт 0.
	f = &triTrimFake{fakeTrim: &fakeTrim{
		groups:  map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 3, LastDeliveredID: "105-0"}}},
		pendErr: map[string]error{StreamDNS + "|dns": errors.New("connection reset")},
	}}
	n, _, err = triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("ошибка XPENDING проглочена")
	}
	if n != 0 {
		t.Errorf("удалено %d при ошибке pending, хочу 0", n)
	}

	// Ошибка XTRIM — ошибка прохода, счёт 0, XLEN не читается
	// (возвращение раньше чтения сигнала).
	f = &triTrimFake{fakeTrim: &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
	}, trimErr: errors.New("OOM command not allowed")}
	n, _, err = triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("ошибка XTRIM проглочена")
	}
	if n != 0 || f.xlenCalls != 0 {
		t.Errorf("XTRIM-сбой: (удалено %d, XLEN %d), хочу (0, 0)", n, f.xlenCalls)
	}

	// Ошибка XLEN — ошибка прохода (счёт стрима при ошибке пассом не
	// принимается: trimmed не растёт, длина — сигнал, не инвариант).
	f = &triTrimFake{fakeTrim: &fakeTrim{
		groups:    map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
		trimCount: 7,
	}, xlenErr: errors.New("connection reset")}
	n, _, err = triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("ошибка XLEN проглочена")
	}
	if n != 0 {
		t.Errorf("удалено %d при ошибке XLEN, хочу 0 (счёт сбойного стрима не принимается)", n)
	}
	// Сам XTRIM до ошибки длины выполнен.
	if len(f.trims) != 1 {
		t.Errorf("XTRIM до ошибки XLEN: %+v, хочу одну обрезку", f.trims)
	}

	// nil-клиент: ошибка с нулевым счётом; цикл — немедленная ошибка.
	n, _, err = TrimOnce(context.Background(), nil, TrimOptions{})
	if err == nil || n != 0 {
		t.Errorf("nil-клиент TrimOnce: (%d, %v), хочу (0, ошибка)", n, err)
	}
	loopErr := make(chan error, 1)
	go func() { loopErr <- TrimLoop(context.Background(), nil, TrimOptions{}) }()
	select {
	case err := <-loopErr:
		if err == nil {
			t.Error("TrimLoop с nil-клиентом не вернул ошибку")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TrimLoop с nil-клиентом не завершился ошибкой")
	}
}

// TestTrimTriagePointAndStuck — точка обрезки на границах: pending
// ровно в одну запись (XINFO Pending 1 и XPENDING Count 1) — точка по
// старейшей pending; гонка «всё подтвердили между XINFO и XPENDING»
// (Count 0) — точка остаётся last-delivered-id группы; сумма обрезки —
// по всем стримам; сигнал застрявшего префикса — ровно на 3/4 предела
// и при явном пределе 1.
func TestTrimTriagePointAndStuck(t *testing.T) {
	// Pending == 1 (граничит с пропуском проверки PEL).
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 1, LastDeliveredID: "105-0"}}},
		pend:   map[string]redis.XPending{StreamDNS + "|dns": {Count: 1, Lower: "101-0"}},
	}
	if _, _, err := triTrimStreamOnce(t, f, TrimOptions{}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "101-0" {
		t.Errorf("XTRIM %+v, хочу MINID 101-0 (pending в одну запись — тоже PEL)", f.trims)
	}

	// Гонка: XINFO показал pending, XPENDING — уже пусто (Count 0):
	// cand остаётся last-delivered-id гоночной группы (меньший id —
	// именно он обязан выиграть минимум; пустой Lower его не затирает).
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {
			{Name: "a", Pending: 1, LastDeliveredID: "50-0"},
			{Name: "b", Pending: 0, LastDeliveredID: "105-0"},
		}},
		pend: map[string]redis.XPending{StreamDNS + "|a": {}}, // Count 0 — гонка
	}
	if _, _, err := triTrimStreamOnce(t, f, TrimOptions{}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "50-0" {
		t.Errorf("XTRIM %+v, хочу MINID 50-0 (пустая перепроверка не сбрасывает точку)", f.trims)
	}

	// Сумма по стримам: два стрима по 7 удалённых — итого 14.
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamDNS:    {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}},
			StreamHTTP:   {{Name: "http", Pending: 0, LastDeliveredID: "43-1"}},
			StreamRender: {{Name: "render", Pending: 0, LastDeliveredID: "44-1"}},
		},
		trimCount: 7,
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamDNS, StreamHTTP, StreamRender},
	})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if n != 21 {
		t.Errorf("удалено %d, хочу 21 (сумма по трём стримам)", n)
	}

	// Ровно 3/4 предела — уже застрявший префикс (length*4 == limit*3).
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
		xlen:   map[string]int64{StreamDNS: 3},
	}
	_, stuck, err := triTrimStreamOnce(t, f, TrimOptions{MaxLen: 4})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(stuck) != 1 {
		t.Errorf("застрявшие %v при XLEN 3/4 = потолок 4, хочу сигнал", stuck)
	}

	// Явный предел 1 — законное значение (дефолт только для 0).
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
		xlen:   map[string]int64{StreamDNS: 2},
	}
	_, stuck, err = triTrimStreamOnce(t, f, TrimOptions{MaxLen: 1})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(stuck) != 1 {
		t.Errorf("застрявшие %v при XLEN 2 > предел 1: явный предел обязан действовать", stuck)
	}
}

// TestTrimTriageNoGroupsNoXLen — стрим без групп (как q:dead): пустой
// проход БЕЗ чтения длины — сигналящий XLEN не зовётся (после удаления
// раннего возврата проход шёл бы читать длину пустому стриму).
func TestTrimTriageNoGroupsNoXLen(t *testing.T) {
	f := &triTrimFake{fakeTrim: &fakeTrim{groups: map[string][]redis.XInfoGroup{}}}
	n, stuck, err := triTrimStreamOnce(t, f, TrimOptions{})
	if err != nil {
		t.Fatalf("TrimOnce без групп: %v", err)
	}
	if n != 0 || len(stuck) != 0 {
		t.Errorf("без групп: (%d, %v), хочу (0, пусто)", n, stuck)
	}
	if f.xlenCalls != 0 {
		t.Errorf("XLEN без групп: %d вызовов, хочу 0", f.xlenCalls)
	}

	// Неизвестный стрим — ошибка с нулевым счётом.
	f2 := &triTrimFake{fakeTrim: &fakeTrim{}}
	n, _, err = TrimOnce(context.Background(), f2, TrimOptions{Streams: []string{"q:typo"}})
	if err == nil || n != 0 {
		t.Errorf("неизвестный стрим: (%d, %v), хочу (0, ошибку)", n, err)
	}
}

// TestTrimTriageLateGroupContributes — группа без доставок (cand == "")
// не ограничивает точку и не прекращает обход: более поздняя группа
// даёт XTRIM по своей точке (break после пустого cand молчал бы).
func TestTrimTriageLateGroupContributes(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {
			{Name: "a", Pending: 0, LastDeliveredID: ""}, // ничего не доставляла
			{Name: "b", Pending: 0, LastDeliveredID: "5-0"},
		}},
	}
	if _, _, err := triTrimStreamOnce(t, f, TrimOptions{}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "5-0" {
		t.Errorf("XTRIM %+v, хочу MINID 5-0 (пустая группа — не конец обхода)", f.trims)
	}
}

// --- tarpit.go -------------------------------------------------------------

// TestTarpitTriageZeroCountOnErrors — все ошибки Track (валидация и
// команды) возвращают строго нулевую мощность: потребитель решает по
// номеру доставки PEL, положительный счёт при ошибке лгал бы.
func TestTarpitTriageZeroCountOnErrors(t *testing.T) {
	ctx := context.Background()
	validation := []struct {
		name     string
		taskID   string
		delivery string
		ttl      time.Duration
	}{
		{"пустой task_id", "", "e1#1", time.Hour},
		{"пустая доставка", "t1", "", time.Hour},
		{"нулевой TTL", "t1", "e1#1", 0},
		{"отрицательный TTL", "t1", "e1#1", -time.Second},
	}
	for _, cs := range validation {
		n, err := NewTarpitCounter(newFakeTarpitRedis()).Track(ctx, cs.taskID, cs.delivery, cs.ttl)
		if err == nil || n != 0 {
			t.Errorf("%s: (%d, %v), хочу (0, ошибку)", cs.name, n, err)
		}
	}
	commandErrs := []struct {
		name   string
		inject func(f *fakeTarpitRedis)
	}{
		{"SADD", func(f *fakeTarpitRedis) { f.saddErr = errors.New("redis: connection closed") }},
		{"EXPIRE", func(f *fakeTarpitRedis) { f.expireErr = errors.New("redis: OOM command not allowed") }},
		{"SCARD", func(f *fakeTarpitRedis) { f.scardErr = errors.New("redis: loading") }},
	}
	for _, cs := range commandErrs {
		f := newFakeTarpitRedis()
		cs.inject(f)
		n, err := NewTarpitCounter(f).Track(ctx, "t1", "e1#1", time.Hour)
		if err == nil || n != 0 {
			t.Errorf("сбой %s: (%d, %v), хочу (0, ошибку)", cs.name, n, err)
		}
	}

	// TTL в 1 нс — законное положительное значение: отметка ставится.
	f := newFakeTarpitRedis()
	if n, err := NewTarpitCounter(f).Track(ctx, "t1", "e1#1", time.Nanosecond); err != nil || n != 1 {
		t.Errorf("TTL 1ns: (%d, %v), хочу (1, nil) — положительный TTL не отбрасывается", n, err)
	}
}

// --- recover.go ------------------------------------------------------------

// TestRecoverLoopTriageNoClaimAfterCancel — полная пачка (n == Batch)
// ведёт цикл сразу к следующему проходу: после отмены ctx обработчиком
// следующий клейм НЕ выполняется — возврат по ctx до пробы и клейма
// (глушение проверки гнало бы клейм по отменённому контексту).
func TestRecoverLoopTriageNoClaimAfterCancel(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
		},
	}
	r := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{MinIdle: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		done <- RecoverLoop(ctx, r, func(c context.Context, _ Delivery) error {
			close(started) // единственная доставка начата
			<-c.Done()     // блокируемся до отмены — цикл в проходе полной пачки
			return nil
		}, RecoverOptions{
			Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
			Batch: 1, Interval: time.Hour, // тик не наступит: выход — только по ctx
		})
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RecoverLoop после отмены: %v, хочу nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RecoverLoop не завершился после отмены ctx")
	}
	// Клейм ровно один: повторного клейма по отменённому ctx нет.
	time.Sleep(50 * time.Millisecond)
	if n := fake.claimsCount(); n != 1 {
		t.Errorf("клеймов после отмены ctx: %d, хочу 1 (выход до пробы записи и клейма)", n)
	}
	if len(fake.dos) != 0 {
		t.Errorf("возвратов попыток после отмены: %d, хочу 0 (хвост пачки не начат)", len(fake.dos))
	}
}

// claimsCount — снимок числа клеймов (fakeClaimRedis.claims без мьютекса
// читается только после завершения горутины цикла — гонки нет).
func (f *fakeClaimRedis) claimsCount() int { return len(f.claims) }

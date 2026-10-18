// Фоновая очистка удалённых доменов (T-020; ТЗ 5.1 delete-ветка, 13.1, 8).
//
// Удалённый домен остаётся в БД со статусом deleted (ТЗ 13.1), а его
// результаты и ссылки на контент убирает фоновая задача: у доменов
// status = deleted удаляются строки subdomain_results, и по каждому
// content_hash удалённых строк уменьшается contents.refcount (ТЗ 6.1:
// content_hash — ссылка на contents; дедупликация — ТЗ 5.7, 8).
//
// Граница с этапом 4: контент с refcount → 0 физически НЕ удаляется —
// пак-файлы и сборка мусора (компактификация паков) остаются за
// хранилищем контента (T-042/T-045, ТЗ 5.7); нулевой refcount лишь
// помечает данные мёртвыми для будущей компактификации. Находки
// laravel_* задача не трогает: план T-020 ограничивает очистку
// результатами и ссылками на контент, а домен остаётся в БД (ТЗ 13.1).
//
// Отслеживание обработанных без смены схемы: «пометка в строке» — сами
// оставшиеся результаты. Кандидат прохода — deleted-домен, у которого
// ещё есть строки subdomain_results (EXISTS); вычищенный домен не
// выбирается вовсе, повторные проходы не делают лишней работы, а сбой
// или рестарт ничего не теряют — внешнего состояния нет. Индекса по
// status в domains нет сознательно («минимум индексов», ТЗ 6): проход
// читает таблицу доменов фильтром; если доля удалённых станет велика,
// решение — частичный индекс WHERE status = 6 (миграция, отдельная
// задача), схему здесь не меняем.
//
// Тот же цикл зачищает осиротевшие партии ingest_stage (T-098, страховка
// ING-P2-3): строки партии, чей конвейер умер без discard (kill -9
// сервиса, падение СУБД), удаляются по возрасту batch_id — время
// создания зашито в старшие байты идентификатора (stage.go), см.
// CleanupStaleBatches.
//
// Момент запуска — по расписанию (ticker в CleanupDeleted), а не событие
// после применения delete-файла: применение (T-019) не связано с
// очисткой API, а повторяемые проходы гарантированно подбирают всё —
// включая результаты, дописанные воркерами после удаления домена
// (ТЗ 8: «…либо удаляется фоновой очисткой»). Подключение к сервису
// ingest — T-021.
package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
)

const (
	// DefaultCleanupBatch — лимит пачки: доменов на один проход.
	// Сопоставимо с пачками планировщика (ТЗ 5.3: 1000–10 000): транзакция
	// прохода остаётся короткой — блокировки строк subdomain_results
	// удерживаются минимум времени, — а темп дренажа при интервале по
	// умолчанию (1000 доменов раз в минуту) — до 60 000 доменов в час.
	DefaultCleanupBatch = 1000

	// DefaultCleanupInterval — интервал проходов цикла CleanupDeleted.
	// ТЗ темп очистки не задаёт: фоновая задача не спешит, данные уже
	// не отдаются клиенту; T-021 сможет завести значение в конфиг.
	DefaultCleanupInterval = time.Minute
)

// contentHashLen — размер content_hash → contents.hash: 16 байт, 128 бит
// (ТЗ 6.1, 5.7). Проверяется при чтении RETURNING — дешёвый инвариант
// против программных ошибок писателя результатов (T-056), как
// MaxLabelLen в Stage. Неверная длина не роняет проход (poison pill,
// ING-P2-4): строка уже удалена, refcount по ней не меняется.
const contentHashLen = 16

// SQL прохода. unnest-пачки — стиль пакетных UPDATE internal/db (ТЗ 6:
// «запись пачками … UPDATE … FROM unnest(...)»).
const (
	// cleanupSelectSQL — кандидаты: deleted-домены с оставшимися
	// результатами. EXISTS — та самая «пометка»: вычищенные не выбираются.
	// ORDER BY по ключу домена — детерминированные пачки без сортировки
	// (совпадает с PK, ТЗ 6.1).
	cleanupSelectSQL = "SELECT d.zone_id, d.id FROM domains d " +
		"WHERE d.status = $1 " +
		"AND EXISTS (SELECT 1 FROM subdomain_results r " +
		"WHERE r.zone_id = d.zone_id AND r.domain_id = d.id) " +
		"ORDER BY d.zone_id, d.id LIMIT $2"

	// cleanupDeleteSQL — удаление результатов пачки доменов. Охрана
	// EXISTS по domains.status — от гонки с применением add-файла
	// (T-018): домен, воскрешённый из deleted в new/pending между
	// выборкой кандидатов и удалением, результаты сохраняет (ТЗ 5.1:
	// вернувшийся домен «не меняется»); домен, удалённый заново, пройдёт
	// цикл ещё раз. RETURNING отдаёт только фактически удалённые строки —
	// уменьшение refcount ниже строго по ним: вторая параллельная очистка,
	// удалившая те же строки раньше, refcount повторно не уменьшит.
	cleanupDeleteSQL = "DELETE FROM subdomain_results r " +
		"USING unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
		"AND EXISTS (SELECT 1 FROM domains d " +
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id AND d.status = $3) " +
		"RETURNING r.zone_id, r.domain_id, r.content_hash"

	// cleanupUpdateSQL — уменьшение refcount по агрегированным хэшам:
	// один контент может ссылаться из многих удалённых результатов
	// (дедупликация, ТЗ 5.7). GREATEST(..., 0) держит счётчик
	// неотрицательным при рассинхронизации (CHECK в схеме нет): минус
	// сломал бы инвариант «0 = мёртвый контент» для компактификации
	// (T-042/T-045). refcount = 0 — не удаление строки: физически
	// носитель чистит компактификация паков (этап 4, T-045).
	cleanupUpdateSQL = "UPDATE contents SET refcount = GREATEST(refcount - h.n, 0) " +
		"FROM unnest($1::bytea[], $2::int4[]) AS h(hash, n) " +
		"WHERE contents.hash = h.hash"
)

// staleStageSQL — зачистка осиротевших партий staging (T-098): диапазон
// batch_id меньше границы «сейчас − StaleBatchAge» — скан по ведущей
// колонке первичного ключа, как и очистка своей партии. Имя таблицы — из
// StageTable (экранирование идентификатора, как в internal/db), единый
// источник для SQL и COPY — поэтому var, как clearStageSQL.
var staleStageSQL = "DELETE FROM " + StageTable.Sanitize() + " WHERE batch_id < $1"

// CleanupDB — пул для очистки: db.DB, из которого можно начать
// транзакцию. Проход атомарен (удаление результатов и уменьшение
// refcount — одна транзакция: сбой между ними не оставит refcount
// навсегда завышенным), поэтому db.DB мало — нужен Begin;
// *pgxpool.Pool удовлетворяет.
type CleanupDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ CleanupDB = (*pgxpool.Pool)(nil)

// CleanupOptions — параметры очистки; нулевые значения заменяются
// дефолтами (withDefaults, стиль watch.Options).
type CleanupOptions struct {
	// BatchSize — максимум доменов в одном проходе (DefaultCleanupBatch);
	// лимит пачки задаёт SELECT кандидатов.
	BatchSize int
	// Interval — период повторных проходов цикла CleanupDeleted
	// (DefaultCleanupInterval); на разовый CleanupOnce не влияет.
	Interval time.Duration
	// Log — логгер цикла CleanupDeleted; nil — записи подавлены
	// (slog.DiscardHandler). CleanupOnce не логирует — итог возвращается
	// CleanupResult.
	Log *slog.Logger
	// OnPass — хук итога успешного прохода: снапшот CleanupResult сразу
	// после фиксации транзакции (ошибочный проход хуку не отдаётся — его
	// уже записал Warn). Точка подключения экспорта метрик (T-099)
	// без зависимости цикла от internal/metrics; стиль AlertsOptions.OnPass
	// (лог + поле). Обязан быть быстрым и не паниковать — вызывается
	// в цикле прохода.
	OnPass func(CleanupResult)
}

// withDefaults заменяет неположительные значения дефолтами.
func (o CleanupOptions) withDefaults() CleanupOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultCleanupBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultCleanupInterval
	}
	return o
}

// CleanupResult — итог одного прохода CleanupOnce.
type CleanupResult struct {
	// Domains — домены, у которых удалены результаты (после охраны от
	// воскрешённых add-файлом; не больше размера пачки).
	Domains int
	// Results — удалённые строки subdomain_results.
	Results int64
	// Contents — строки contents с уменьшенным refcount (различные хэши).
	Contents int64
	// BadHashes — удалённые результаты с content_hash неверной длины
	// (контракт contents — 16 октет, ТЗ 6.1): проход не роняется,
	// refcount по ним не менялся; цикл CleanupDeleted помечает это Warn.
	BadHashes int64
}

// cleanupKey — ключ домена: партиция + идентификатор, как PK domains.
type cleanupKey struct {
	zoneID int16
	id     int64
}

// cleanupHash — 128-битный content_hash (ТЗ 6.1); ключ агрегации ссылок.
type cleanupHash [contentHashLen]byte

// CleanupOnce выполняет один проход очистки (ТЗ 5.1, 13.1): до
// opts.BatchSize доменов в статусе deleted, у которых ещё есть
// результаты, — в одной транзакции удаляются их строки
// subdomain_results, и по каждому content_hash удалённых строк
// уменьшается contents.refcount. Повторный проход безопасен (ТЗ 3.4):
// вычищенные домены не выбираются, refcount уменьшается строго по
// фактически удалённым ссылкам.
func CleanupOnce(ctx context.Context, pool CleanupDB, opts CleanupOptions) (CleanupResult, error) {
	if pool == nil {
		return CleanupResult{}, errors.New("ingest: очистка удалённых доменов: пул не задан")
	}
	o := opts.withDefaults()

	zoneIDs, domainIDs, err := cleanupCandidates(ctx, pool, o.BatchSize)
	if err != nil {
		return CleanupResult{}, err
	}
	if len(zoneIDs) == 0 {
		// Пустой набор — чистить нечего; транзакция не открывается.
		return CleanupResult{}, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return CleanupResult{}, fmt.Errorf("ingest: очистка удалённых доменов: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op. Прерванный проход не
	// оставляет половинчатых изменений (ТЗ 3.4).
	defer func() { _ = tx.Rollback(ctx) }()

	res, refs, err := cleanupResults(ctx, tx, zoneIDs, domainIDs)
	if err != nil {
		return CleanupResult{}, err
	}
	if len(refs) > 0 {
		n, err := cleanupRefcounts(ctx, tx, refs)
		if err != nil {
			return CleanupResult{}, err
		}
		res.Contents = n
	}
	if err := tx.Commit(ctx); err != nil {
		return CleanupResult{}, fmt.Errorf("ingest: очистка удалённых доменов: фиксация: %w", err)
	}
	return res, nil
}

// cleanupCandidates выбирает до limit deleted-доменов с оставшимися
// результатами (cleanupSelectSQL): параллельные массивы ключей зон и
// доменов для unnest-пачки DELETE.
func cleanupCandidates(ctx context.Context, pool CleanupDB, limit int) ([]int16, []int64, error) {
	rows, err := pool.Query(ctx, cleanupSelectSQL, int16(model.StatusDeleted), limit)
	if err != nil {
		return nil, nil, fmt.Errorf("ingest: очистка удалённых доменов: выборка кандидатов: %w", err)
	}
	defer rows.Close()

	var zoneIDs []int16
	var domainIDs []int64
	for rows.Next() {
		var (
			zoneID   int16
			domainID int64
		)
		if err := rows.Scan(&zoneID, &domainID); err != nil {
			return nil, nil, fmt.Errorf("ingest: очистка удалённых доменов: чтение кандидатов: %w", err)
		}
		zoneIDs = append(zoneIDs, zoneID)
		domainIDs = append(domainIDs, domainID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("ingest: очистка удалённых доменов: выборка кандидатов: %w", err)
	}
	return zoneIDs, domainIDs, nil
}

// cleanupResults удаляет строки subdomain_results пачки доменов
// (cleanupDeleteSQL) и возвращает счётчики прохода вместе с числом
// удалённых ссылок на каждый хэш контента.
func cleanupResults(ctx context.Context, tx pgx.Tx, zoneIDs []int16, domainIDs []int64) (CleanupResult, map[cleanupHash]int32, error) {
	rows, err := tx.Query(ctx, cleanupDeleteSQL, zoneIDs, domainIDs, int16(model.StatusDeleted))
	if err != nil {
		return CleanupResult{}, nil, fmt.Errorf("ingest: очистка удалённых доменов: удаление результатов: %w", err)
	}
	defer rows.Close()

	domains := make(map[cleanupKey]struct{}, len(zoneIDs))
	refs := make(map[cleanupHash]int32)
	var res CleanupResult
	for rows.Next() {
		var (
			zoneID   int16
			domainID int64
			hash     []byte
		)
		if err := rows.Scan(&zoneID, &domainID, &hash); err != nil {
			return CleanupResult{}, nil, fmt.Errorf("ingest: очистка удалённых доменов: чтение удалённых результатов: %w", err)
		}
		domains[cleanupKey{zoneID: zoneID, id: domainID}] = struct{}{}
		res.Results++
		switch {
		case hash == nil:
			// Результат без контента (NULL content_hash) — ссылок не держит.
		case len(hash) != contentHashLen:
			// Программная ошибка писателя результатов (T-056): строка уже
			// удалена, ронять проход нельзя — один некорректный хэш блокировал
			// бы очистку навсегда (кандидаты перевыбираются те же, ING-P2-4).
			// Refcount по такой ссылке не трогаем, итог — BadHashes.
			res.BadHashes++
		default:
			var h cleanupHash
			copy(h[:], hash)
			refs[h]++
		}
	}
	if err := rows.Err(); err != nil {
		return CleanupResult{}, nil, fmt.Errorf("ingest: очистка удалённых доменов: удаление результатов: %w", err)
	}
	res.Domains = len(domains)
	return res, refs, nil
}

// cleanupRefcounts уменьшает contents.refcount по числу удалённых ссылок
// (cleanupUpdateSQL) и возвращает число затронутых строк contents.
func cleanupRefcounts(ctx context.Context, tx pgx.Tx, refs map[cleanupHash]int32) (int64, error) {
	list := make([]cleanupHash, 0, len(refs))
	for h := range refs {
		list = append(list, h)
	}
	// Детерминированный порядок аргументов — воспроизводимые пачки.
	sort.Slice(list, func(i, j int) bool {
		return bytes.Compare(list[i][:], list[j][:]) < 0
	})
	hashes := make([][]byte, len(list))
	counts := make([]int32, len(list))
	for i, h := range list {
		hashes[i] = h[:]
		counts[i] = refs[h]
	}

	tag, err := tx.Exec(ctx, cleanupUpdateSQL, hashes, counts)
	if err != nil {
		return 0, fmt.Errorf("ingest: очистка удалённых доменов: уменьшение refcount contents: %w", err)
	}
	return tag.RowsAffected(), nil
}

// StaleBatchAge — порог осиротевших партий staging (T-098). После фикса
// ING-P2-3 (614e690) сбойную партию чистит discard собственным контекстом,
// но kill -9 сервиса или падение СУБД оставляют строки навсегда:
// UNLOGGED-таблица переживает рестарт сервиса, а усекается только
// аварийным восстановлением PostgreSQL (crash recovery) — штатный рестарт
// СУБД строки сохраняет. Партия живёт ровно время обработки одного
// файла — конвейер обрабатывает файлы последовательно, а пачки COPY
// дают ~10⁵–10⁶ строк/с, поэтому даже огромный файл укладывается
// в минуты: сутки — заведомый запас поверх времени жизни любой живой
// партии, зачистка не может подрезать партию работающего Process
// (гонка исключена возрастом, а не блокировкой). Осиротевшие строки
// на корректность не влияют (batch_id не встретится повторно) — только
// место на диске, так что большой запас дешевле риска; конфигурационного
// ключа не заводим (экспорт — для интеграционного теста границы).
//
// Граница контракта — системные часы (возраст и границы — wall time):
// шаг времени вперёд больше порога в момент живой обработки (пауза ВМ
// дольше суток, ручная правка даты, запуск до синхронизации времени)
// преждевременно состаривает партию. Самое частое осложнение длинной
// паузы ВМ — обрыв соединения с БД — самолечится: Process вернёт
// OutcomeLeft, файл переработается со свежим batch_id. Исключение
// активных партий из зачистки (реестр, разделяемый конвейером и циклом)
// сознательно не делаем: стоимость состояния несоизмерима со сценарием,
// часы узлов обязаны быть согласованы (NTP/chrony в режиме slew).
//
// Строки, оставшиеся от версий до T-098 (полностью случайный batch_id),
// под диапазон времени почти не попадают и не удаляются вовсе — до
// аварийного восстановления PostgreSQL, усекающего UNLOGGED-таблицу;
// на корректность это не влияет, только место на диске.
const StaleBatchAge = 24 * time.Hour

// CleanupStaleBatches удаляет из staging строки партий старше StaleBatchAge
// (T-098) и возвращает их число. Возраст читается из самого batch_id —
// времени создания в старших байтах (stage.go): колонки-метки времени в
// staging нет, а диапазон по префиксу идентификатора обслуживается
// первичным ключом. Живые партии моложе порога не задеваются; пустой
// staging — DELETE 0.
func CleanupStaleBatches(ctx context.Context, pool db.DB, now time.Time) (int64, error) {
	if pool == nil {
		return 0, errors.New("ingest: зачистка осиротевших партий staging: пул не задан")
	}
	// Граница возраста обязана выражаться положительными микросекундами:
	// до 1970 года (часы на эпохе Unix — севшая батарейка RTC, старт до
	// синхронизации) uint64-переход в batchIDBefore сделал бы границу
	// огромной и зачистка срезала бы весь staging. Отказ (Warn в цикле)
	// вместо удаления всего — проверяется именно граница now−StaleBatchAge,
	// а не само now: часы на эпохе 1970-01-01 дают отрицательную границу.
	if cutoff := now.Add(-StaleBatchAge); cutoff.UnixMicro() < 0 {
		return 0, errors.New("ingest: зачистка осиротевших партий staging: граница возраста до 1970 года — системные часы не синхронизированы")
	}
	cutoff := batchIDBefore(now.Add(-StaleBatchAge))
	tag, err := pool.Exec(ctx, staleStageSQL, cutoff[:])
	if err != nil {
		return 0, fmt.Errorf("ingest: зачистка осиротевших партий %s: %w", StageTable, err)
	}
	return tag.RowsAffected(), nil
}

// CleanupDeleted — фоновая задача очистки (ТЗ 5.1, 13.1): повторяет
// CleanupOnce по ticker (первый проход — сразу: дренаж накопленного
// после рестарта) до отмены ctx и возвращает nil — контракт OnRun каркаса
// сервисов (T-012: чистое завершение по SIGTERM). Теми же проходами
// зачищаются осиротевшие партии staging (CleanupStaleBatches, T-098).
// Ошибка прохода не останавливает задачу и сервис: логируется (Warn),
// очередной проход повторит — проходы идемпотентны (ТЗ 3.4).
func CleanupDeleted(ctx context.Context, pool CleanupDB, opts CleanupOptions) error {
	if pool == nil {
		return errors.New("ingest: очистка удалённых доменов: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("фоновая очистка удалённых доменов",
		"interval", o.Interval.String(), "batch", o.BatchSize)

	pass := func() {
		res, err := CleanupOnce(ctx, pool, o)
		switch {
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход очистки удалённых доменов", "err", err)
		default:
			if res.BadHashes > 0 {
				// Poison pill обезврежен (ING-P2-4): аномалия видна оператору,
				// очистка продолжается.
				log.Warn("удалены результаты с некорректным content_hash — refcount не менялся",
					"bad_hashes", res.BadHashes)
			}
			if res.Domains > 0 {
				log.Info("проход очистки удалённых доменов",
					"domains", res.Domains, "results", res.Results, "contents", res.Contents)
			}
			// Итог прохода — потребителям (метрика T-099): после лога,
			// до зачистки staging — строго итог CleanupOnce.
			if o.OnPass != nil {
				o.OnPass(res)
			}
		}

		// Зачистка осиротевших партий staging (T-098) — тем же проходом
		// цикла: первый проход сразу после старта подбирает строки,
		// осиротевшие при гибели предыдущего запуска (kill -9, падение
		// СУБД). Ошибка не останавливает цикл — повторит очередной
		// проход; нулевой итог не логируется.
		if rows, err := CleanupStaleBatches(ctx, pool, time.Now()); err != nil {
			if ctx.Err() == nil {
				log.Warn("зачистка осиротевших партий staging", "err", err)
			}
		} else if rows > 0 {
			log.Warn("удалены осиротевшие строки staging",
				"rows", rows, "older_than", StaleBatchAge.String())
		}
	}

	pass()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

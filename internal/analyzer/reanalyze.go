// Package analyzer — анализ сохранённого контента по правилам Laravel
// (ТЗ 5.8): пассивный, по уже скачанным страницам, cookies и заголовкам.
// Потребление q:analyze и запись laravel_findings — T-071 (worker.go).
//
// Этот файл — переанализ (T-074, ТЗ 5.8 «Переанализ»: «фоновая задача
// находит записи с устаревшей версией правил и ставит их в q:analyze;
// повторное скачивание не требуется»; приёмка §11.4 п. 4 — «изменение
// правил + reanalyze обновляет результаты без повторного скачивания»).
//
// Критерий устаревания — rules_version IS DISTINCT FROM текущей версии
// набора (rules.Set.Version, T-068), а не «<>»: NULL («никогда не
// анализировалась» — строки, записанные до T-071, и новые эпизоды
// проверки T-058) выпадали бы из сравнения NULL <> x = NULL. Охрана
// §13.1 — deleted-домены вне цикла (status <> deleted, стиль выборок
// scheduler T-030; их результаты чистит T-020). Отключённые зоны не
// фильтруются: §5.10/§4.1 выключение зоны останавливает новые проверки,
// переанализ уже сохранённых результатов сеть не трогает и правил не
// нарушает.
//
// Повторное скачивание не выполняется: сообщение Result (T-024) несёт
// только ключ строки (домен, субдомен, вариант) и имя для логов; воркер
// T-071 берёт контент из хранилища по ключу.
//
// Дренаж пачками с ключевой пагинацией. Соседние фоновые задачи (T-020/
// T-030/T-066) повторяют проход, пока строки удовлетворяют условию — их
// оператор сам выводит строки из отбора. Публикация q:analyze строку не
// меняет: rules_version зафиксирует воркер, асинхронно; повторная
// выборка «с начала» брала бы ту же пачку in-flight строк и дублировала
// её с каждой итерацией. Поэтому дренаж идёт курсором по ключу строки
// (row-value > , порядок PK): одно полное прохождение множества за цикл,
// строки, ставшие устаревшими после прохождения (новый эпизод проверки),
// подберёт следующий проход. Дубликаты между проходами ограничены
// (in-flight окно) и гасятся идемпотентностью потребителя по ключу —
// как у T-066.
//
// Backpressure — WatermarkPublisher (T-026): публикация ждёт ёмкости
// q:analyze (единый порог scheduler.queue_high_watermark), отмена ctx —
// чистый выход. Опубликованное до сбоя не откатывается (at-least-once,
// §3.4): повторный проход опубликует снова — дубликат ограничен.
//
// Контракт с воркером T-071: по каждому обработанному Result воркер
// фиксирует subdomain_results.rules_version = версия набора — в том
// числе строкам без сохранённого контента («анализировать нечего» —
// тоже результат с версией), иначе фоновый проход возвращался бы к ним
// бесконечно.
package analyzer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// Дефолты прохода (стиль T-020/T-030/T-066: константы, отдельной опции
// конфига нет — ТЗ 5.10 состав фиксирован; рабочие значения цикл
// получает опциями для тестов и вызова из CLI).
const (
	// DefaultReanalyzeInterval — период фонового прохода. Смена правил
	// обрабатывается немедленно — по уведомлению Reloader (Subscribe,
	// T-068); тикер — страховка: потерянные сообщения (усечение
	// MAXLEN ~, T-025), сбой воркера, строки новых эпизодов проверки.
	// Час, а не минута (стиль T-020/T-066): проход без индекса по
	// rules_version сканирует таблицу и в спокойном состоянии пуст —
	// лишние полные сканы ни к чему; и не сутки (стиль T-030):
	// страховка не должна ждать перепроверочного интервала.
	DefaultReanalyzeInterval = time.Hour

	// DefaultReanalyzeBatch — строк в одной пачке: проход читает только
	// ключи (контент берёт воркер по ключу), 5000 совпадает с пачками
	// планировщика (DefaultRecheckBatch, T-030) — короткая выборка и
	// ограниченная серия публикаций до проверки ёмкости гейтом.
	DefaultReanalyzeBatch = 5000
)

// reanalyzeSelectSQL — пачка устаревших строк: версия отлична от текущей
// (IS DISTINCT FROM: NULL — никогда не анализировалась), домен не
// удалён (§13.1), зона — опционально (NULL параметр — все). Курсор
// row-value > по ключу строки (PK) отсекает уже пройденное дренажем;
// ORDER BY по тому же ключу — детерминированные пачки и корректная
// пагинация. Имя — label || '.' || зона (стиль T-027) для Result.Name
// и логов (ТЗ 4.2).
const reanalyzeSelectSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::int2, $5::int8, $6::int2, $7::int2) " +
	"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $8"

// zoneIDByNameSQL — идентификатор зоны по имени для фильтра команды
// crawlerctl reanalyze --zone: имя хранится в punycode (EnsureZone,
// T-014), UNIQUE — не больше одной строки.
const zoneIDByNameSQL = "SELECT id FROM zones WHERE name = $1"

// RulesSource — источник набора правил для фонового цикла; удо­влетворяется
// *rules.Reloader (T-068). Узкий интерфейс — цикл не знает о SIGHUP и
// файлах, юнит-тесты подменяют фейком.
type RulesSource interface {
	// Set — текущий неизменяемый снапшот (nil не бывает у Reloader;
	// фейк без набора — проход пропускается с Warn).
	Set() *rules.Set
	// Subscribe — уведомления о смене набора: внеочередной проход.
	Subscribe() (<-chan *rules.Set, func())
}

// ReanalyzeOptions — параметры переанализа; нулевые значения заменяются
// дефолтами (withDefaults, стиль RecheckOptions T-030).
type ReanalyzeOptions struct {
	// Interval — период цикла ReanalyzeLoop (DefaultReanalyzeInterval);
	// на разовые проходы не влияет.
	Interval time.Duration
	// BatchSize — максимум строк в одной пачке (DefaultReanalyzeBatch).
	BatchSize int
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o ReanalyzeOptions) withDefaults() ReanalyzeOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultReanalyzeInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultReanalyzeBatch
	}
	return o
}

// ReanalyzeResult — итог прохода: строки выбраны/опубликованы. Разница
// Published < Selected — ошибка публикации посреди пачки (проход
// остановлен).
type ReanalyzeResult struct {
	Selected  int
	Published int
}

// resultKey — ключ строки subdomain_results (PK, ТЗ 6.1) и курсор
// пагинации дренажа.
type resultKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant model.Variant
}

// reanalyzeRow — строка пачки: ключ и имя домена для сообщения.
type reanalyzeRow struct {
	key  resultKey
	name string
}

// ReanalyzeOnce — один проход: первая пачка устаревших строк — выборка
// до opts.BatchSize и публикация Result в q:analyze по каждой (ТЗ 5.8
// «Переанализ»). Повторное скачивание не выполняется. Проход идемпотентен
// на уровне «выборка+публикация»: после фиксации воркером T-071 новой
// rules_version строки перестают отбираться.
func ReanalyzeOnce(ctx context.Context, pool db.DB, q queue.Queue, version int32, zone *int16, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	rows, err := reanalyzeSelect(ctx, pool, version, zone, resultKey{}, opts.withDefaults().BatchSize)
	if err != nil {
		return ReanalyzeResult{}, err
	}
	out := ReanalyzeResult{Selected: len(rows)}
	if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
		return out, err
	}
	return out, nil
}

// ReanalyzeDrain — полный дренаж устаревшего множества пачками
// (DefaultReanalyzeBatch): выборка → публикация → курсор за последнюю
// строку пачки, до первой неполной пачки. Так стартовый дренаж цикла и
// команда crawlerctl reanalyze проходят всё множество без повторов
// внутри прохода; «дорасти» до следующего прохода могут только строки,
// ставшие устаревшими после прохождения курсора.
func ReanalyzeDrain(ctx context.Context, pool db.DB, q queue.Queue, version int32, zone *int16, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	o := opts.withDefaults()
	out := ReanalyzeResult{}
	after := resultKey{} // нулевой ключ меньше любого реального (zone_id ≥ 1)
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("analyzer: переанализ: %w", err)
		}
		rows, err := reanalyzeSelect(ctx, pool, version, zone, after, o.BatchSize)
		if err != nil {
			return out, err
		}
		out.Selected += len(rows)
		if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
			return out, err
		}
		if len(rows) < o.BatchSize {
			return out, nil // множество исчерпано
		}
		after = rows[len(rows)-1].key
	}
}

// reanalyzeSelect — пачка устаревших строк после курсора after.
func reanalyzeSelect(ctx context.Context, pool db.DB, version int32, zone *int16, after resultKey, limit int) ([]reanalyzeRow, error) {
	var zoneArg any // nil → NULL параметр → фильтр зоны выключен
	if zone != nil {
		zoneArg = *zone
	}
	rows, err := pool.Query(ctx, reanalyzeSelectSQL,
		version,
		int16(model.StatusDeleted),
		zoneArg,
		int16(after.zone), int64(after.domain), int16(after.sub), int16(after.variant),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка: %w", err)
	}
	defer rows.Close()

	var out []reanalyzeRow
	for rows.Next() {
		var r reanalyzeRow
		var variant int16
		if err := rows.Scan(&r.key.zone, &r.key.domain, &r.key.sub, &variant, &r.name); err != nil {
			return nil, fmt.Errorf("analyzer: переанализ: выборка: чтение: %w", err)
		}
		r.key.variant = model.Variant(variant)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка: %w", err)
	}
	return out, nil
}

// reanalyzePublish публикует Result по каждой строке пачки (q:analyze,
// формат T-024): только ключ и имя — контент воркер возьмёт из хранилища.
func reanalyzePublish(ctx context.Context, q queue.Queue, rows []reanalyzeRow, out *ReanalyzeResult) error {
	for i := range rows {
		msg := queue.Result{
			ZoneID:      rows[i].key.zone,
			DomainID:    rows[i].key.domain,
			SubdomainID: rows[i].key.sub,
			Variant:     rows[i].key.variant,
			Name:        rows[i].name,
		}
		if _, err := q.Publish(ctx, queue.StreamAnalyze, msg); err != nil {
			return fmt.Errorf("analyzer: переанализ: публикация q:analyze (домен %s, запись %d/%d): %w",
				rows[i].name, i+1, len(rows), err)
		}
		out.Published++
	}
	return nil
}

// ReanalyzeLoop — фоновый цикл переанализа (ТЗ 5.8 «Фоновая задача»):
// стартовый дренаж пачками (подбирает устаревшее за простоем — стиль
// T-020/T-030/T-066), далее тикер Interval и внеочередной дренаж по
// смене набора правил (Reloader.Subscribe, T-068: SIGHUP без рестарта).
// Каждый дренаж берёт актуальную версию из src.Set(). Ошибка прохода —
// Warn, цикл живёт (контракт OnRun, T-012): опубликованное не
// теряется, неопубликованное останется устаревшим — следующий дренаж
// начнёт с него. Отмена ctx — чистый nil (graceful shutdown, §3.4).
func ReanalyzeLoop(ctx context.Context, pool db.DB, q queue.Queue, src RulesSource, opts ReanalyzeOptions) error {
	if pool == nil {
		return errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return errors.New("analyzer: переанализ: очередь не задана")
	}
	if src == nil {
		return errors.New("analyzer: переанализ: источник набора правил не задан (rules.Reloader)")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	sets, cancelSub := src.Subscribe()
	defer cancelSub()
	log.Info("цикл переанализа запущен",
		"interval", o.Interval.String(), "batch", o.BatchSize)

	// drain проходит всё устаревшее множество пачками с курсором;
	// версия — актуальная на момент старта дренажа.
	drain := func() {
		set := src.Set()
		if set == nil {
			log.Warn("проход переанализа: набор правил не загружен — пропуск")
			return
		}
		res, err := ReanalyzeDrain(ctx, pool, q, set.Version, nil, o)
		if err != nil {
			// Отмена во время прохода — часть остановки, не тревога.
			if ctx.Err() == nil {
				log.Warn("проход переанализа", "err", err,
					"selected", res.Selected, "published", res.Published)
			}
			return
		}
		if res.Published > 0 {
			log.Info("проход переанализа",
				"rules_version", set.Version,
				"selected", res.Selected, "published", res.Published)
		}
	}

	drain()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case set, ok := <-sets:
			if !ok {
				// Эквивалентный мутант (T-M10): вырезание sets = nil
				// оставляет в select закрытый канал — он по-прежнему
				// немедленно готов, ok=false и continue вращают цикл
				// вхолостую без drain и логов, состояние и дальнейшие
				// развилки те же (различие только в нагрузке CPU —
				// детерминированно не наблюдаемо). Вырезание ветки
				// целиком закрыто тестом TestReanalyzeLoopClosedSets
				// (поток drain/логов по закрытому каналу).
				// mutator-disable-next-line statement/remove
				sets = nil // закрытый канал не должен зациклить select
				// Эквивалентный мутант (T-M10): continue → break лишь
				// выходит из select — цикл for продолжает следующей
				// итерацией, а тело ветки и так исчерпано: неразличимо.
				// mutator-disable-next-line loop/break
				continue
			}
			log.Info("смена набора правил — внеочередной переанализ",
				"rules_version", set.Version)
			drain()
		case <-ticker.C:
			drain()
		}
	}
}

// ZoneIDByName — идентификатор зоны по punycode-имени для фильтра
// команды crawlerctl reanalyze --zone. found = false — зоны в
// справочнике нет (ingest создаёт зоны при первом файле, T-014).
func ZoneIDByName(ctx context.Context, pool db.DB, name string) (id int16, found bool, err error) {
	if pool == nil {
		return 0, false, errors.New("analyzer: зона по имени: пул не задан")
	}
	rows, err := pool.Query(ctx, zoneIDByNameSQL, name)
	if err != nil {
		return 0, false, fmt.Errorf("analyzer: зона по имени: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, false, fmt.Errorf("analyzer: зона по имени: чтение: %w", err)
		}
		return id, true, nil
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("analyzer: зона по имени: %w", err)
	}
	return 0, false, nil
}

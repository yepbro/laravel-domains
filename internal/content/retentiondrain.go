// Состояние дренажа retention, переживающее проходы (T-185; ТЗ
// TZ_slow_retention_drain.md §4.2–4.4 — диагноз запроса №13 журнала
// медленных 24.10: ежечасные полные круги по PK перечитывали heap всей
// пройденной истории, дорожка росла с таблицей ≈3 млн строк/сут, медиана
// 76 с, 63 мин времени БД за 2,4 ч).
//
// Модель «один раз за жизнь» (§3): курсор по (checked_at, PK)
// инициализируется нулём при старте процесса — первый проход и есть
// catch-up-круг по накопленному (платится рестартом, не часом) — и
// продвигается пачками вперёд; очищенная строка остаётся позади курсора,
// перепроверённая (DELETE+INSERT, fresh checked_at — T-058) входит в
// дренаж заново, когда состарится. «Неполная пачка» — дренаж до конца
// текущего окна старения; следующий проход (час) продолжает с места
// остановки — срез состарившегося за час ≈ приток, окно обслуживает
// индекс subdomain_results_checked_idx (T-171).
//
// Явные ретраи вместо полных кругов (§4.3–4.4): ежечасный круг был
// одновременно ретраем (подбирал защищённые) и дорожкой (перечитывал
// историю) — замена:
//
//   - bounded-список ключей, отложенных защитой очередей
//     (retentionpending.go): следующий проход предлагает их точечным
//     зондом по PK (retentionProbeSQL), всё ещё защищённые остаются в
//     списке; потеря списка (рестарт) покрывается catch-up-кругом
//     старта; переполнение — Warn и консервативный catch-up-круг;
//   - catch-up-круг по обнулению счётчика рендер-долга
//     crawler_counters['render_pending'] (T-175): записи под флагом
//     render_pending молча отфильтровываются JOIN'ом — курсор по времени
//     их проходит; переход счётчика в 0 (вся рендер-защита снята разом)
//     читается точечно в начале каждого прохода и планирует один полный
//     круг с нулевого курсора. Отсутствие строки счётчика (БД до
//     деплоя T-175) — fallback: круг раз в сутки (константа, конфиг-поле
//     не заводится, ТЗ §4.4).
//
// Что не меняется (§4.5): purge/decref/delete с охранными повторными
// проверками, интерлок retentionBlocked, снимки retentionPendingSnapshot
// и их обновление длинного дренажа, статистики и метрики OnPass.
package content

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// DefaultRetentionRetryLimit — предел списка ключей ретраев защиты
	// очередей (T-185): порядок глубин стримов у watermark'ов (гейт
	// T-026: 100 тыс.; render-порог T-174: 10 тыс.) — застой большего
	// масштаба упразднёнными кругами не вылечить, интерлок глубины
	// останавливает проходы раньше. Память ключа ≈ десятки байт —
	// сотни тысяч строк списка остаются дёшевы; превышение — Warn и
	// консервативный catch-up-круг следующим проходом (круг пересоберёт
	// список сам). Тесты подменяют полем retentionState.retryLimit.
	DefaultRetentionRetryLimit = 100000

	// DefaultRetentionFallbackCircle — период fallback-круга при
	// недоступном счётчике рендер-долга (нет строки либо таблицы —
	// БД до деплоя T-175, сбой чтения; ТЗ §4.4): сутки — «раз в
	// спокойный час» без конфиг-поля; якорь двигают успешные чтения
	// счётчика и выполненные круги. «Спокойный час» ТЗ понимается как
	// «сутки от якоря», а не час суток: отдельное расписание заведено
	// сознательно не было (конфиг-поле запрещено ТЗ).
	DefaultRetentionFallbackCircle = 24 * time.Hour
)

// retentionState — дренажное состояние цикла, переживающее проходы
// (T-185). Владелец — цикл RetentionLoop (одна горутина тикера);
// разовый RetentionOnce работает с чистым состоянием. Nil-карта
// retries — «ретраев нет».
type retentionState struct {
	// cursor — курсор дренажа по времени (§4.2): последняя посещённая
	// строка; продвигается только зафиксированными пачками.
	cursor retentionCursor
	// retries — bounded-множество ключей, отложенных защитой очередей
	// (§4.3): следующий проход предлагает их зондом retentionProbeSQL.
	retries map[retentionKey]struct{}
	// debtSeen — счётчик рендер-долга наблюдался > 0 после последнего
	// круга (§4.4): переход следующего чтения в 0 — catch-up-круг.
	debtSeen bool
	// lastCircle — якорь fallback-суток: успешные чтения счётчика долга
	// и выполненные круги (fallback значит «счётчик недоступен сутки
	// ПОДРЯД», транзиентный блип круга не поднимает); начальный якорь —
	// создание состояния (стартовый дренаж и так с нуля).
	lastCircle time.Time
	// noCounterTable — таблицы crawler_counters ещё не было (Info один
	// раз на процесс, ревью T-185, P3-4 R3).
	noCounterTable bool
	// circle — следующий проход стартует с нулевого курсора (§4.2–4.4):
	// снятие рендер-долга, переполнение списка ретраев, fallback-сутки;
	// circleWhy — причина для журнала.
	circle    bool
	circleWhy string

	// retryLimit — предел списка ретраев (DefaultRetentionRetryLimit);
	// поле для юнит-тестов — меньше возиться с сотней тысяч ключей.
	retryLimit int
}

// limit — эффективный предел списка ретраев.
func (st *retentionState) limit() int {
	if st.retryLimit > 0 {
		return st.retryLimit
	}
	return DefaultRetentionRetryLimit
}

// retentionPass — один проход цикла с состоянием st (ТЗ §5.1):
// catch-up-решение по счётчику рендер-долга в начале прохода, точечные
// предложения ретраев, дренаж пачками от курсора st.cursor до
// исчерпания окна старения, остановки интерлоком, ошибки защиты очередей
// или отмены ctx. Остановки сохраняют прогресс: курсор и список ретраев
// переживут проход — следующий продолжит с места остановки.
func retentionPass(ctx context.Context, pool RetentionDB, opts RetentionOptions, st *retentionState) (RetentionStats, error) {
	if pool == nil {
		return RetentionStats{}, errors.New("content: retention: пул не задан")
	}
	if st == nil {
		st = &retentionState{}
	}
	// Якорь fallback-суток — момент создания состояния: стартовый дренаж
	// и так идёт с нулевого курсора («круг только что был»); без якоря
	// первый же транзиентный сбой чтения счётчика считался бы «сутки
	// истекли» и немедленно поднимал полный круг (ревью T-185, P3-3).
	if st.lastCircle.IsZero() {
		st.lastCircle = time.Now()
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	st.catchUp(ctx, pool, log)
	var out RetentionStats
	// Снимок защиты — лениво, ПОСЛЕ первой проверки интерлока: интерлок
	// «анализатор не успевает» закрывает и q:analyze, и q:render шире
	// снимка — гонять тяжёлый XRANGE ради тут же выброшенного прохода
	// незачем; кроме того, таймаут чтения зависшего стрима при закрытом
	// интерлоке дал бы ложный SkippedPending вместо честного Skipped.
	// Ошибка чтения — не ошибка прохода, а его отмена (данные не тронуты):
	// см. шапку retentionpending.go.
	var pending *retentionPendingSets
	selects := 0 // выборок пачек с момента снимка (обновление длинного дренажа)
	probed := false
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("content: retention: %w", err)
		}
		// Интерлок: анализатор не успевает — тела и headers/cookies
		// записей очереди ещё нужны; проверяется на каждой пачке —
		// длинный дренаж видит свежую глубину.
		n, blocked, err := retentionBlocked(ctx, o)
		if err != nil {
			return out, fmt.Errorf("content: retention: интерлок q:analyze: %w", err)
		}
		if blocked {
			out.Skipped = true
			out.Depth = n
			return out, nil
		}
		// Первый снимок — после интерлока, до первой выборки: чистка
		// начинается только когда известно, какие записи ждут очередей.
		if o.Pending != nil && pending == nil {
			p, err := retentionPendingSnapshot(ctx, o.Pending)
			if err != nil {
				if ctx.Err() != nil {
					return out, fmt.Errorf("content: retention: снимок очередей: %w", ctx.Err())
				}
				out.SkippedPending = true
				return out, nil
			}
			pending = p
		}

		// Точечные предложения ретраев (§4.3) — строго один раз за проход,
		// до дренажа: список ниже курсора, зонд не зависит от пачек; ключи,
		// отложенные этим же проходом, предлагаются следующим.
		if !probed {
			probed = true
			if len(st.retries) > 0 {
				if err := retentionRetryPass(ctx, pool, o, st, pending, &out); err != nil {
					return out, err
				}
			}
		}

		batch, err := retentionSelect(ctx, pool, o, st.cursor)
		if err != nil {
			return out, err
		}
		if len(batch) == 0 {
			return out, nil // окно старения исчерпано — курсор ждёт следующего часа
		}
		selects++
		// Курсор и решение об исчерпании — по исходной выборке: исключённые
		// защитой записи этим проходом пропускаются (курсор за ними) и
		// вернутся точечным предложением (§4.3) — чистка только
		// не-ожидающих.
		last, full := batch[len(batch)-1], len(batch) >= o.BatchSize
		st.deferRetries(batch, pending, log)
		batch = pending.deferBatch(batch, &out)
		if len(batch) > 0 {
			stp, err := retentionPurge(ctx, pool, o, batch)
			if err != nil {
				return out, err
			}
			out.add(stp)
		}
		st.cursor = retentionCursor{checkedAt: last.checkedAt, key: last.key}
		if !full {
			return out, nil // неполная пачка — дренаж исчерпан
		}
		// Длинный дренаж: снимок устарел (гонка переиздания T-066) —
		// перечитать дешевле, чем допустить окно; обновление покрывает
		// только будущие пачки, пройденное не пересматривается.
		if o.Pending != nil && selects >= DefaultRetentionPendingRefresh {
			p, err := retentionPendingSnapshot(ctx, o.Pending)
			if err != nil {
				if ctx.Err() != nil {
					return out, fmt.Errorf("content: retention: снимок очередей: %w", ctx.Err())
				}
				out.SkippedPending = true
				return out, nil
			}
			pending, selects = p, 0
		}
	}
}

// catchUp — решение о catch-up-круге в начале прохода (§4.4): точечное
// чтение счётчика рендер-долга и применение отложенного круга
// (переполнение списка ретраев, fallback-сутки). Недоступный счётчик —
// нет строки, нет таблицы (42P01: БД до деплоя миграции 000015 —
// трактуется как «счётчика нет», без Warn) либо сбой чтения: fallback
// страховкой-сутками, якорь которых двигают успешные чтения — fallback
// значит «недоступен сутки ПОДРЯД», транзиентный блип после недель
// здоровой работы полного круга не поднимает (ревью T-185, P3-2 R3).
// Дискретизация часа: эпоха долга, целиком вместившаяся между чтениями
// (набрал и дренировал в ноль), перехода в 0 не увидит — её записи
// догоняет первый наблюдаемый ноль после (круг с нуля чистит всё позади
// курсора), переполнение списка, fallback либо рестарт; триггер
// «существенное уменьшение долга» отвергнут сознательно: длинный дренаж
// (443 тыс., этап 25) уменьшался бы каждый час — круги вернулись бы.
func (st *retentionState) catchUp(ctx context.Context, pool RetentionDB, log *slog.Logger) {
	if debt, ok, err := retentionDebt(ctx, pool); err != nil && errors.Is(err, errRetentionNoCounterTable) {
		// Таблицы счётчика нет (миграция 000015 не применена) — штатное
		// состояние старого стенда: один Info на процесс, дальше тишина
		// (ревью T-185, P3-4 R3: полное молчание отложило бы диагноз
		// «миграция не применилась» на сутки до первого круга).
		if !st.noCounterTable {
			st.noCounterTable = true
			log.Info("retention: таблицы счётчика рендер-долга нет — работаем fallback-сутками (миграция 000015 не применена?)")
		}
		st.fallbackCircle()
	} else if err != nil {
		if ctx.Err() == nil {
			log.Warn("retention: счётчик рендер-долга не прочитан — состояние долга держится, страховка fallback-суток",
				"err", err)
		}
		st.fallbackCircle()
	} else if !ok {
		// Строки счётчика нет (БД до деплоя T-175) — тот же fallback.
		st.fallbackCircle()
	} else {
		st.lastCircle = time.Now() // якорь fallback-суток: чтение удалось
		if debt < 0 {
			// Кодом не порождается (переходы бита ведут пару ±1, T-175) —
			// рассинхрон ручного SQL (runbooks §6); трактуется как «долга
			// нет», но громко (стиль renderDebtCount T-181).
			log.Warn("retention: отрицательный счётчик рендер-долга — рассинхрон (runbooks §6)", "debt", debt)
		}
		if debt > 0 {
			st.debtSeen = true
		} else if st.debtSeen {
			// Переход в 0: вся рендер-защита снята — один полный круг
			// догоняет всё, что копилось за эпоху долга позади курсора.
			st.planCircle("счётчик рендер-долга обнулился — рендер-защита снята")
		}
	}

	if !st.circle {
		return
	}
	// Круг: курсор в начало, список ретраев пересоберёт сам круг
	// (защищённые им же отложатся), эпоха долга закрыта. Noop-круг
	// (курсор уже нулевой, списка нет) — Debug: стартовое состояние
	// свежего процесса, операции он не меняет.
	why := st.circleWhy
	noop := st.cursor == (retentionCursor{}) && len(st.retries) == 0
	st.cursor = retentionCursor{}
	st.retries = nil
	st.debtSeen = false
	st.lastCircle = time.Now()
	st.circle, st.circleWhy = false, ""
	if noop {
		log.Debug("retention: catch-up-круг уже на месте (нулевой курсор)", "reason", why)
		return
	}
	log.Info("retention: catch-up-круг с нулевого курсора", "reason", why)
}

// fallbackCircle — страховка-сутки при недоступном счётчике долга
// (§4.4): круг раз в DefaultRetentionFallbackCircle от якоря (создание
// состояния либо последний круг). «Спокойный час» ТЗ понимается как
// «сутки от якоря», а не час суток: якорь двигают сами круги, отдельного
// расписания заведено сознательно не было (конфиг-поле запрещено ТЗ).
func (st *retentionState) fallbackCircle() {
	if time.Since(st.lastCircle) >= DefaultRetentionFallbackCircle {
		st.planCircle("счётчик рендер-долга недоступен (нет строки/таблицы — миграция 000015) — fallback-сутки")
	}
}

// planCircle — плановый catch-up-круг следующим видом применения
// (началом текущего прохода); повторные причины не затирают первую.
func (st *retentionState) planCircle(why string) {
	if !st.circle {
		st.circle, st.circleWhy = true, why
	}
}

// deferRetries — пополнение списка ретраев ключами пачки, отложенными
// защитой очередей (§4.3): следующий проход предложит их точечно.
// Переполнение — Warn и консервативный catch-up-круг следующим
// проходом: список ограничен памятью, круг пересоберёт всё множество
// (превышение — масштаб застоя стримов у watermark'ов, где полные круги
// всё равно были бы единственным механизмом).
func (st *retentionState) deferRetries(batch []retentionCandidate, pending *retentionPendingSets, log *slog.Logger) {
	if pending == nil {
		return // защита выключена — отложений нет
	}
	for _, c := range batch {
		if !pending.waiting(c.key) {
			continue
		}
		if st.retries == nil {
			st.retries = make(map[retentionKey]struct{})
		}
		if _, ok := st.retries[c.key]; ok {
			continue
		}
		if len(st.retries) >= st.limit() {
			// Warn — один раз за эпизод (до применения круга): дальше
			// план уже стоит, тишина до следующего переполнения (ревью
			// T-185, P3-1 — не шуметь каждой пачкой застоя).
			if !st.circle {
				log.Warn("retention: список ретраев защиты очередей переполнен — следующий проход catch-up-кругом",
					"limit", st.limit())
			}
			st.planCircle("список ретраев защиты очередей переполнен — круг пересоберёт множество")
			return
		}
		st.retries[c.key] = struct{}{}
	}
}

// retentionRetryPass — точечные предложения ключей ретраев (§4.3):
// зонд retentionProbeSQL по PK возвращает ещё кандидатов очистки;
// свежий снимок защиты отфильтровывает всё ещё ждущих (ключ остаётся в
// списке на следующий проход), остальные чистятся пачками BatchSize.
// Ключи, не вернувшиеся зондом, из списка выпадают: защита ушла и
// очистка прошла иным механизмом (круг), строка переписана (fresh
// checked_at — вернётся в дренаж, когда состарится), стала находкой
// либо ждёт рендера (догонит catch-up-круг по счётчику долга).
// Идемпотентно: сбой чистки — зафиксированные пачки остаются, зонд
// следующего прохода не вернёт уже очищенное.
func retentionRetryPass(ctx context.Context, pool RetentionDB, o RetentionOptions, st *retentionState, pending *retentionPendingSets, out *RetentionStats) error {
	keys := make([]retentionKey, 0, len(st.retries))
	for k := range st.retries {
		keys = append(keys, k)
	}
	cands, err := retentionProbe(ctx, pool, o, keys)
	if err != nil {
		return err
	}
	// Новый список — всё ещё защищённые; чистке — остальные. Исключение
	// зонда считается в Deferred по стримам-держателям — та же
	// наблюдаемость, что у пачек дренажа (ключ честно ждёт потребителей,
	// сколько бы проходов его ни предлагали).
	next := make(map[retentionKey]struct{}, len(st.retries))
	var waiting, purge []retentionCandidate
	for _, c := range cands {
		if pending != nil && pending.waiting(c.key) {
			waiting = append(waiting, c)
			continue
		}
		purge = append(purge, c)
	}
	_ = pending.deferBatch(waiting, out) // исключает все (ждущие) и считает Deferred
	for _, c := range waiting {
		next[c.key] = struct{}{}
	}
	// Замена списка — только после чистки целиком: сбой среди пачек
	// держит прежний список (неочищенные ключи не теряются позади
	// курсора), следующий проход повторит зонд идемпотентно.
	for i := 0; i < len(purge); i += o.BatchSize {
		j := min(i+o.BatchSize, len(purge))
		stp, err := retentionPurge(ctx, pool, o, purge[i:j])
		if err != nil {
			return err
		}
		out.add(stp)
	}
	st.retries = next
	return nil
}

// errRetentionNoCounterTable — таблицы crawler_counters нет (SQLSTATE
// 42P01, БД до деплоя миграции 000015): сентинел для catchUp —
// обрабатывается как «счётчика нет» с одним Info на процесс, а не как
// сбой чтения с Warn (ревью T-185, P3-2/P3-4 R3).
var errRetentionNoCounterTable = errors.New("content: retention: таблицы счётчика рендер-долга нет (миграция 000015 не применена)")

// retentionDebt — точечное чтение счётчика рендер-долга
// crawler_counters['render_pending'] (retentionDebtSQL, T-175).
// Второй возврат ok=false — счётчика нет: строки (БД до деплоя T-175)
// либо таблицы — тогда err = errRetentionNoCounterTable (42P01).
// Не ошибка прохода: caller уходит в fallback-сутки (ТЗ §4.4); в
// отличие от guard прохода доработки (renderDebtCount, T-181), для
// которого отсутствие строки — ошибка: здесь чтение оптимизационное,
// круг дороже пустого промаха.
func retentionDebt(ctx context.Context, pool RetentionDB) (n int64, ok bool, err error) {
	rows, err := pool.Query(ctx, retentionDebtSQL)
	if err != nil {
		// Таблицы нет — БД до деплоя миграции 000015 (SQLSTATE 42P01,
		// undefined_table — литералом, коды схемы в проекте пишутся
		// литералами, стиль record.go): штатное переходное состояние
		// «счётчика нет», как отсутствие строки (ревью T-185, P3-2:
		// Warn на каждый часовой проход старого стенда — шум).
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return 0, false, errRetentionNoCounterTable
		}
		return 0, false, fmt.Errorf("content: retention: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// Сначала сбой чтения: обрыв соединения — не «строки нет»
		// (стиль renderDebtCount T-181).
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("content: retention: %w", err)
		}
		return 0, false, nil
	}
	if err := rows.Scan(&n); err != nil {
		return 0, false, fmt.Errorf("content: retention: счётчик рендер-долга: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("content: retention: %w", err)
	}
	return n, true, nil
}

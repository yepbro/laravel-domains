// Поздний переиздатель рендера (T-259, ТЗ-fsm §3 ФТ-5.3; спецификация
// автомата рендер-состояния строки — doc-комментарий
// internal/renderworker/worker.go, этот файл — писатель перехода П4):
// фоновый проход, переиздающий в q:render «вечно молодые» записи — rendered = false,
// final_url IS NOT NULL, render_failed_at IS NULL — старше 6 ч, у домена
// которых нет бита render_pending (ими занимается проход доработки
// renderq.go) и нет непотреблённых сообщений q:render/q:render-fallback
// (интерлок — снимок стримов по образцу content/retentionpending.go:
// XRANGE недоставленного хвоста от last-delivered-id группы, адаптер —
// cmd/http-worker retentionPending, тот же, что у защиты retention).
//
// Что лечит: потерю MAXLEN-усечённых фолбэк-задач (воркер A опубликовал
// q:render-fallback, T-238; обрезка T-025 удалила запись до доставки —
// запись без маркера и без сообщений неотличима от «не пробовали») и
// потерянные публикации q:render (сбой/рестарт Redis между XADD и
// доставкой). Состояние записи перестаёт зависеть от выживания сообщения
// в стриме: пока запись не отрендерена и не отмаркирована, переиздатель
// вернёт ей задачу. Дубли ограничены интервалом и гасятся
// идемпотентностью потребителя (ТЗ 3.4, как у прохода доработки).
//
// SPA-вердикт — по сохранённому контенту (DetectSPA T-057, детерминирован
// над теми же байтами, что HTTP-этап проверял при записи): предикат
// возраста выбирает СУПЕРМНОЖЕСТВО (не-SPA записи подходят под колонки
// неограниченно долго — рендер им не нужен, §5.5 п. 8 публикует только
// SPA), издание без вердикта залило бы q:render перепроверкой всех
// страниц датасета. Поэтому проход повторяет решение прохода доработки
// (renderDomainKeys): своё тело — свой вердикт, алиас наследует вердикт
// субдомена-оригинала (класс эквивалентности T-055), записи без
// собственного контента и без алиаса не переиздаются вовсе — вердикт
// взять неоткуда (контент срезан retention T-166 либо не сохранён,
// T-056), их доработает следующий эпизод перепроверки T-030 (то же
// осознанное ограничение, что у прохода доработки: старше окна retention
// контента нет — ходить туда бессмысленно).
//
// Курсорная пагинация по PK (стиль reanalyzeSelectSQL T-074/T-193:
// row-value > по ключу строки + ORDER BY PK + LIMIT): проход обязан
// ГАРАНТИРОВАННО продвигаться по всему множеству кандидатов — без
// курсора LIMIT без сортировки (стиль T-181) возвращал бы первую пачку
// кучи вечно (не-SPA кандидаты не покидают множество — проход не
// дошёл бы до потерянных записей за ними). Курсор живёт в цикле
// (RenderLateCursor), за пачкой обработанных — включая исключённых
// интерлоком/вердиктом (их держатели/эпизоды решат сами, полный цикл
// вернётся), короткая пачка замыкает цикл на начало. Полный цикл по
// таблице (~40 млн строк) при пачке 1000/мин — порядка месяца,
// быстрее естественной перепроверки доменов (scheduler.recheck_interval,
// дефолт ~6 мес): потерянная задача восстанавливается задолго до
// перезаписи эпизода; ход по строкам без контента (старее retention) —
// дешёвый index-скан без чтения паков.
//
// Курсор не двигается за пачку, которую не удалось отработать целиком
// (полная очередь — Full, ошибка публикации/снимка): следующий тик
// повторит пачку; уже опубликованная часть повторится дублем —
// ограничено пачкой, гасится идемпотентностью потребителя (ТЗ 3.4).
// Ошибка чтения снимка стримов — консервативный пропуск прохода, как у
// защиты retention: переиздавать «наугад» дороже, чем отложить на тик
// (дубль живой задачи — именно то, что интерлок обязан исключать).
//
// Наблюдаемость: счётчик crawler_render_late_republish_total
// (httpworker.Metrics.LateRepublished) — стационарно не растёт; рост
// после инцидента (обрезка/потеря Redis) — переиздатель дренирует
// потерянное, спад до нуля — дренаж завершён.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// Дефолты прохода (стиль T-066 renderq.go: константы, отдельной опции
// конфига нет — ТЗ 5.10 состав фиксирован; рабочие значения цикл
// получает опциями для тестов).
const (
	// DefaultRenderLateInterval — период тика цикла: минута, как
	// DefaultRenderRequeueInterval. Проход — страховка потери (не штатный
	// путь публикаций), чаще незачем: окно «старше 6 ч» переживает
	// минутный тик с запасом; при полной очереди проход стоит одно чтение
	// глубины.
	DefaultRenderLateInterval = time.Minute

	// DefaultRenderLateBatch — строк в одной пачке. Курсор обязан пройти
	// всё множество кандидатов (супермножество — почти вся таблица)
	// быстрее естественного перепроверочного цикла доменов (дефолт
	// ~6 мес): 1000/мин ≈ 1,4 млн строк/сутки. Вердикт читает тело из
	// паков — только у строк с контентом (внутри окна retention; в
	// среднем десятки КиБ — умеренный фоновый I/O рядом с
	// retention-пачками того же сервиса); строки без контента фильтруются
	// без чтения.
	DefaultRenderLateBatch = 1000

	// DefaultRenderLateAfter — возраст «вечно молодой» записи (ТЗ-fsm
	// ФТ-5.3, дефолт 6 ч): моложе — задачу могли ещё не доставить
	// (нормальный лаг очереди при глубине до порога T-174 ≈ 2 ч буфера),
	// переиздание раньше срока плодило бы дубли живых задач. 6 ч — за
	// пределами любых штатных окон доставки (ретраи PEL — минуты,
	// T-025), до перезаписи эпизода перепроверкой (T-030,
	// scheduler.recheck_interval) — месяцы запаса на полный курсорный
	// цикл прохода.
	DefaultRenderLateAfter = 6 * time.Hour
)

// SQL прохода. Коды схемы — литералами (стиль T-039/renderq.go); бит
// render_pending — model.FlagRenderPending (бит 1), deleted —
// model.StatusDeleted.
var (
	// lateRenderRowsSQL — пачка кандидатов после курсора (PK-курсор
	// T-074/T-193): не отрендерено, есть цель рендера, нет маркера
	// «рендер невозможен» (T-210 — финализации закрыли запись до смены
	// контента), возраст за окном DefaultRenderLateAfter (checked_at —
	// час эпизода, T-058; границу считает Go-часами — расхождение часов
	// приложения и БД на фоне окна 6 ч несущественно, стиль якоря
	// T-193 §4.2), домен без бита render_pending (долгом занимается
	// проход доработки renderq.go — дубль изданий запрещён делением
	// владения) и не deleted (§13.1 — вне цикла проверок). JOIN zones —
	// имя для Result.Name (стиль T-027). Курсор row-value > по PK
	// отсекает пройденное, ORDER BY по тому же ключу — детерминированные
	// пачки и корректная пагинация; ведущие колонки PK — партиции
	// LIST/HASH (T-005) режут поиск до листа. Форма плана НЕ закрепляется
	// (в отличие от выборки долга T-181): курсор гарантирует продвижение —
	// патологии вытаптывания дренированного префикса нет, пачка читает
	// только свой диапазон индекса; на малых объёмах планировщик берёт
	// Sort по подходящим строкам, на больших — ordered index scan с ранним
	// остановом — контролировать журналом медленных после деплоя (стиль
	// T-177). content_hash и
	// alias_subdomain_id — вердикт (DetectSPA T-057 / наследование
	// T-055); hash NULL — тела нет, alias NULL — наследовать не у кого.
	lateRenderRowsSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
		"r.content_hash, r.alias_subdomain_id, d.label || '.' || z.name " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"JOIN zones AS z ON z.id = r.zone_id " +
		"WHERE r.rendered = false AND r.final_url IS NOT NULL AND r.render_failed_at IS NULL " +
		"AND r.checked_at < $1::timestamptz " +
		"AND (d.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") = 0 " +
		"AND d.status <> " + fmt.Sprintf("%d", int16(model.StatusDeleted)) + " " +
		"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($2::int2, $3::int8, $4::int2, $5::int2) " +
		"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
		"LIMIT $6"

	// lateRenderAliasSQL — строки субдоменов-оригиналов для наследования
	// вердикта алиасами пачки (T-055): кандидаты (не отрендерено, есть
	// цель, нет маркера — те же охранные предикаты, что у
	// renderPendingRowsSQL прохода доработки, вердикт по телу) ПЛЮС
	// отрендеренные оригиналы: сам факт rendered = true доказывает
	// SPA-вердикт класса (публикации q:render идут только по SPA —
	// renderSPAResults/renderDomainKeys/наш вердикт), контент такого
	// оригинала — уже DOM и DetectSPA по нему неверен; ревью T-259 P2-1:
	// без отрендеренных оригиналов алиас с потерянным сообщением (ровно
	// класс «потерянная фолбэк-задача», publishFallback публикует по
	// строке) не восстанавливался вовсе. Отмаркированный оригинал
	// (rendered = false + маркер) вердикта не даёт — отмаркированные
	// записи не переиздаются (семантика T-210, как у прохода доработки).
	// subdomain_id = ANY — пачка алиасов одной команды в один запрос;
	// ORDER BY — детерминизм.
	lateRenderAliasSQL = "SELECT subdomain_id, variant, content_hash, rendered FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = ANY($3::int2[]) " +
		"AND (rendered OR (final_url IS NOT NULL AND render_failed_at IS NULL)) " +
		"ORDER BY subdomain_id, variant"
)

// lateRenderStreams — стримы интерлока: непотреблённые сообщения любого
// из них означают живую задачу записи (q:render — основной путь и
// переиздания, q:render-fallback — «ждёт фолбэка», T-238) — запись не
// переиздаётся. Имена — константы пакета очередей (единственный
// источник, как retentionPendingStreams в content).
var lateRenderStreams = []string{queue.StreamRender, queue.StreamRenderFallback}

// RenderLateCursor — курсор прохода по PK: последняя посещённая строка,
// переживающая проходы цикла (образец ReanalyzeCursor T-193). Нулевое
// значение — «с начала таблицы» (zone_id ≥ 1 больше нулевого ключа).
// Поля не экспортированы: курсор двигает только LateOnce, владелец —
// цикл RenderLateLoop; внешние вызовы держат значение целиком.
type RenderLateCursor struct {
	key lateKey
}

// lateKey — ключ строки subdomain_results (PK, ТЗ 6.1) и курсор.
type lateKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant model.Variant
}

// lateRow — строка пачки: ключ, тело для вердикта (nil — контента нет),
// алиас (не nil — вердикт наследуется от оригинала), имя для сообщения.
type lateRow struct {
	key      lateKey
	hash     []byte
	aliasSub *int16
	name     string
}

// LateOptions — параметры позднего переиздателя; нулевые значения
// заменяются дефолтами (withDefaults, стиль RequeueOptions T-066).
type LateOptions struct {
	// Interval — период цикла RenderLateLoop (DefaultRenderLateInterval);
	// на разовый LateOnce не влияет.
	Interval time.Duration
	// BatchSize — максимум строк в одной пачке
	// (DefaultRenderLateBatch).
	BatchSize int
	// After — возраст «вечно молодой» записи (DefaultRenderLateAfter).
	After time.Duration
	// Log — логгер цикла и Warn-ы прохода; nil — записи подавлены
	// (slog.DiscardHandler).
	Log *slog.Logger
}

func (o LateOptions) withDefaults() LateOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRenderLateInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRenderLateBatch
	}
	if o.After <= 0 {
		o.After = DefaultRenderLateAfter
	}
	return o
}

// LateResult — итог одного прохода LateOnce.
type LateResult struct {
	// Selected — строк выбрано пачкой (после курсора, под предикаты).
	Selected int
	// Skipped — исключено без издания: живая задача (интерлок снимка
	// стримов), не-SPA-вердикт, нет источника вердикта (контент срезан/
	// не сохранён, алиас без оригинала-кандидата), нечитаемый блок.
	// Курсор проходит мимо: их держатели и следующий цикл решат сами.
	Skipped int
	// Published — переиздано сообщений q:render (метрика
	// crawler_render_late_republish_total растёт на это число). Счёт —
	// пачки, подтверждённой целиком: сбой Publish посреди пачки оставляет
	// опубликованный префикс в стриме неучтённым (дубль ограничен пачкой,
	// гасится идемпотентностью, ТЗ 3.4; следующий тик опубликует пачку
	// снова — тогда счёт честный).
	Published int
	// Full — q:render у порога: проход остановлен ДО публикации, курсор
	// не двигался — следующий тик повторит пачку.
	Full bool
	// Wrapped — пачка короче лимита: множество за курсором исчерпано,
	// курсор замкнут на начало таблицы (следующий проход — с нуля).
	Wrapped bool
}

// LateOnce — один проход позднего переиздателя: пачка кандидатов после
// курсора (предикат lateRenderRowsSQL) → интерлок снимком непотреблённых
// q:render/q:render-fallback (живая задача — не трогаем) → SPA-вердикт
// по сохранённому контенту (своё тело / наследование алиаса) →
// переиздание q:render при свободной очереди. Курсор двигается за пачку,
// отработанную целиком (изданное + исключённое); короткая пачка замыкает
// цикл. cursor nil — разовый вызов с нулевого ключа без сохранения.
//
// Порядок «снимок → издание» держит окно гонки минимальным: публикация
// после снимка (живая задача появилась между ними) даёт дубль —
// ограничено пачкой, гасится идемпотентностью потребителя (ТЗ 3.4);
// сообщение, доставленное группе ДО снимка, снимком не видно (хвост
// читается от last-delivered-id, образец retention) — такая задача в
// PEL воркера и вернётся клеймом/дублем в пределах минут (T-025).
func LateOnce(ctx context.Context, pool db.DB, contents RenderContent, render *RenderQueue,
	pending content.RetentionPending, met *Metrics, opts LateOptions, cursor *RenderLateCursor) (LateResult, error) {
	if pool == nil {
		return LateResult{}, errors.New("httpworker: поздний переиздатель: пул не задан")
	}
	if contents == nil {
		return LateResult{}, errors.New("httpworker: поздний переиздатель: хранилище контента не задано")
	}
	if render == nil {
		return LateResult{}, errors.New("httpworker: поздний переиздатель: очередь рендера не задана")
	}
	if pending == nil {
		return LateResult{}, errors.New("httpworker: поздний переиздатель: источник непотреблённых ключей стримов не задан (интерлок обязателен, образец retentionpending.go)")
	}
	if cursor == nil {
		cursor = &RenderLateCursor{} // разовый вызов: курсор без сохранения
	}
	o := opts.withDefaults()
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	out := LateResult{}

	// Полная очередь — БД не читается (одно чтение глубины): переиздание
	// в полный стрим — усиление переполнения (записи рисковали бы
	// MAXLEN-обрезкой снова); сигнал «стало свободно» — следующий тик.
	free, err := render.HasCapacity(ctx)
	if err != nil {
		return out, fmt.Errorf("httpworker: поздний переиздатель: ёмкость q:render: %w", err)
	}
	if !free {
		return LateResult{Full: true}, nil
	}

	rows, err := lateSelect(ctx, pool, time.Now().Add(-o.After), cursor.key, o.BatchSize)
	if err != nil {
		return out, err
	}
	if len(rows) == 0 {
		// За курсором кандидатов нет — цикл замкнут: следующая пачка
		// начнёт таблицу заново (за тик могли появиться новые потери
		// позади курсора — им не ждать полного цикла).
		cursor.key = lateKey{}
		return LateResult{Wrapped: true}, nil
	}
	out.Selected = len(rows)

	// Интерлок: снимок непотреблённых ключей защищаемых стримов — ОДИН
	// на пачку (стиль retentionPendingSnapshot). Ошибка любого стрима —
	// ошибка прохода целиком: частичная защита хуже пропуска (живая
	// задача q:render-fallback без снимка была бы переиздана дублем).
	keys := make(map[content.RetentionPendingKey]struct{})
	for _, s := range lateRenderStreams {
		ks, err := pending.PendingKeys(ctx, s)
		if err != nil {
			return out, fmt.Errorf("httpworker: поздний переиздатель: непотреблённые %s: %w", s, err)
		}
		for k := range ks {
			keys[k] = struct{}{}
		}
	}

	publish := make([]queue.Result, 0, len(rows))
	// Кэш вердиктов по хэшу контента: записи класса эквивалентности
	// T-055 делят тело — тело читается и вердиктится один раз на пачку.
	verdict := make(map[content.Hash]bool)
	// Алиасы пачки — вердикт от оригиналов одним запросом на домен
	// (lateRenderAliasSQL принимает пачку субдоменов ANY).
	type aliasRef struct {
		row  lateRow
		orig int16
	}
	origSubs := make(map[lateKey][]int16) // key (zone, domain) → субдомены-оригиналы
	var aliases []aliasRef
	for i := range rows {
		r := &rows[i]
		live := content.RetentionPendingKey{Zone: r.key.zone, Domain: r.key.domain, Sub: r.key.sub, Variant: int16(r.key.variant)}
		if _, ok := keys[live]; ok {
			// Живая задача: сообщение ждёт потребителя — запись не
			// трогаем (критерий приёмки ФТ-5.3).
			out.Skipped++
			continue
		}
		if r.aliasSub != nil {
			aliases = append(aliases, aliasRef{row: *r, orig: *r.aliasSub})
			dom := r.key
			dom.sub, dom.variant = 0, 0
			origSubs[dom] = append(origSubs[dom], *r.aliasSub)
			continue
		}
		if len(r.hash) != content.HashLen {
			// Контента нет (T-056 либо срезан retention T-166) — вердикт
			// взять неоткуда; ограничение прохода доработки, шапка файла.
			out.Skipped++
			continue
		}
		var h content.Hash
		copy(h[:], r.hash)
		spa, err := lateBodySPA(ctx, contents, verdict, h, r, o.Log)
		if err != nil {
			return out, err
		}
		if !spa {
			out.Skipped++ // не-SPA: рендер не требуется (§5.5 п. 8)
			continue
		}
		publish = append(publish, lateResult(*r))
	}

	// Вердикты оригиналов алиасов пачки: один запрос на домен, тела —
	// через тот же кэш (оригинал и алиас делят класс T-055 — но контент
	// у алиаса NULL, читается тело оригинала).
	origSPA := make(map[lateKey]bool) // (zone, domain, orig sub) → SPA
	for dom, subs := range origSubs {
		spa, err := lateOriginsSPA(ctx, pool, contents, verdict, dom, subs, o.Log)
		if err != nil {
			return out, err
		}
		for sub, ok := range spa {
			origSPA[lateKey{zone: dom.zone, domain: dom.domain, sub: sub}] = ok
		}
	}
	for _, a := range aliases {
		if origSPA[lateKey{zone: a.row.key.zone, domain: a.row.key.domain, sub: a.orig}] {
			publish = append(publish, lateResult(a.row))
			continue
		}
		out.Skipped++ // оригинал не кандидат/не SPA/не читается —
		// наследовать нечего: следующий цикл/эпизод решат
	}

	if len(publish) > 0 {
		// Ёмкость перед самой публикацией: снимок и вердикты пачки шли
		// минуты — очередь могла наполниться (проход доработки
		// проверяет так же перед каждым доменом).
		free, err := render.HasCapacity(ctx)
		if err != nil {
			return out, fmt.Errorf("httpworker: поздний переиздатель: ёмкость q:render: %w", err)
		}
		if !free {
			out.Full = true
			return out, nil
		}
		if err := render.Publish(ctx, publish); err != nil {
			return out, fmt.Errorf("httpworker: поздний переиздатель: %w", err)
		}
		out.Published = len(publish)
		met.LateRepublished(out.Published)
	}

	// Пачка отработана целиком — курсор за последнюю строку (включая
	// исключённых: интерлок/вердикт решают их судьбу без прохода).
	cursor.key = rows[len(rows)-1].key
	if len(rows) < o.BatchSize {
		cursor.key = lateKey{} // короткая пачка — множество исчерпано, цикл замкнут
		out.Wrapped = true
	}
	return out, nil
}

// lateResult — Result-сообщение переиздания (ключ строки + имя, T-024):
// потребитель читает цель из строки (renderSelectSQL), свежесть
// гарантируют охраны эпизода.
func lateResult(r lateRow) queue.Result {
	return queue.Result{
		ZoneID: r.key.zone, DomainID: r.key.domain,
		SubdomainID: r.key.sub, Variant: r.key.variant, Name: r.name,
	}
}

// lateBodySPA — вердикт SPA по телу контента h с кэшем пачки: попадание
// в кэш не читает хранилище (записи класса T-055 делят тело). Ошибка
// чтения: отмена ctx — ошибка прохода; транзиентный сбой — false без
// ошибки (строка пропускается, следующий цикл вернётся), Warn в лог.
func lateBodySPA(ctx context.Context, contents RenderContent, cache map[content.Hash]bool, h content.Hash, r *lateRow, log *slog.Logger) (bool, error) {
	if spa, ok := cache[h]; ok {
		return spa, nil
	}
	body, err := contents.Get(ctx, h)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false, fmt.Errorf("httpworker: поздний переиздатель: чтение контента: %w", err)
		}
		log.Warn("поздний переиздатель: контент записи недоступен — строка пропущена",
			"domain", r.name, "subdomain_id", r.key.sub, "err", err)
		cache[h] = false // сбой не вердикт: не-читаемость не «не-SPA»,
		// но повторное чтение в этой пачке бессмысленно; цена пропуска —
		// весь курсорный цикл до возврата к строке (осознанно: ошибка
		// прохода застопорила бы курсор на нечитаемом блоке навсегда)
		return false, nil
	}
	spa := httpfetch.DetectSPA(body).SPA
	cache[h] = spa
	return spa, nil
}

// lateOriginsSPA — вердикты субдоменов-оригиналов алиасов одного домена
// (T-055): кандидаты (lateRenderAliasSQL — охранные предикаты прохода
// доработки) вердиктятся по телу; отрендеренный оригинал — вердикт сам
// по себе (факт рендера доказывает SPA класса, контент-то уже DOM —
// ревью T-259 P2-1), без чтения хранилища. Любой вариант оригинала с
// SPA-вердиктом делает класс эквивалентности SPA (у прохода доработки
// та же семантика: наследуются оба варианта разных сайтов).
// Возвращает карту субдомен → вердикт; пустая — наследовать нечего.
// Ошибки чтения тел — как у lateBodySPA (пропуск оригинала).
func lateOriginsSPA(ctx context.Context, pool db.DB, contents RenderContent, cache map[content.Hash]bool,
	dom lateKey, subs []int16, log *slog.Logger) (map[int16]bool, error) {
	rows, err := pool.Query(ctx, lateRenderAliasSQL, dom.zone, dom.domain, subs)
	if err != nil {
		return nil, fmt.Errorf("httpworker: поздний переиздатель: оригиналы алиасов: %w", err)
	}
	defer rows.Close()
	out := make(map[int16]bool, len(subs))
	for rows.Next() {
		var (
			sub      int16
			variant  int16
			hash     []byte
			rendered bool
		)
		if err := rows.Scan(&sub, &variant, &hash, &rendered); err != nil {
			return nil, fmt.Errorf("httpworker: поздний переиздатель: оригиналы алиасов: чтение: %w", err)
		}
		if rendered {
			out[sub] = true // рендер состоялся — класс был SPA, тело не читаем
			continue
		}
		if len(hash) != content.HashLen {
			continue // вердикт взять неоткуда
		}
		var h content.Hash
		copy(h[:], hash)
		row := lateRow{name: "origin"} // для Warn в lateBodySPA
		spa, err := lateBodySPA(ctx, contents, cache, h, &row, log)
		if err != nil {
			return nil, err
		}
		if spa {
			out[sub] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: поздний переиздатель: оригиналы алиасов: %w", err)
	}
	return out, nil
}

// lateSelect — пачка кандидатов после курсора after (lateRenderRowsSQL).
func lateSelect(ctx context.Context, pool db.DB, before time.Time, after lateKey, limit int) ([]lateRow, error) {
	rows, err := pool.Query(ctx, lateRenderRowsSQL,
		before,
		int16(after.zone), int64(after.domain), int16(after.sub), int16(after.variant),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("httpworker: поздний переиздатель: выборка: %w", err)
	}
	defer rows.Close()

	var out []lateRow
	for rows.Next() {
		var (
			r        lateRow
			variant  int16
			hash     []byte
			aliasSub *int16
		)
		if err := rows.Scan(&r.key.zone, &r.key.domain, &r.key.sub, &variant, &hash, &aliasSub, &r.name); err != nil {
			return nil, fmt.Errorf("httpworker: поздний переиздатель: выборка: чтение: %w", err)
		}
		r.key.variant, r.hash, r.aliasSub = model.Variant(variant), hash, aliasSub
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: поздний переиздатель: выборка: %w", err)
	}
	return out, nil
}

// RenderLateLoop — цикл позднего переиздателя: тикер Interval, один
// проход LateOnce на тик (полный курсорный цикл по таблице —
// часы/дни; стартового дренажа сознательно нет: гонять всю таблицу
// пачками подряд — часы фонового I/O на старте сервиса). Курсор —
// состояние цикла (одна горутина, стиль retentionState T-185). Ошибка
// прохода — Warn, цикл живёт (контракт OnRun, T-012): курсор стоит на
// месте, следующий тик повторит пачку. Отмена ctx — чистый nil
// (graceful shutdown, §3.4).
func RenderLateLoop(ctx context.Context, pool db.DB, contents RenderContent, render *RenderQueue,
	pending content.RetentionPending, met *Metrics, opts LateOptions) error {
	if pool == nil {
		return errors.New("httpworker: поздний переиздатель: пул не задан")
	}
	if contents == nil {
		return errors.New("httpworker: поздний переиздатель: хранилище контента не задано")
	}
	if render == nil {
		return errors.New("httpworker: поздний переиздатель: очередь рендера не задана")
	}
	if pending == nil {
		return errors.New("httpworker: поздний переиздатель: источник непотреблённых ключей стримов не задан")
	}
	o := opts.withDefaults()
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	log := o.Log
	log.Info("поздний переиздатель рендера запущен",
		"interval", o.Interval.String(), "batch", o.BatchSize, "after", o.After.String())

	cursor := &RenderLateCursor{}
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			res, err := LateOnce(ctx, pool, contents, render, pending, met, o, cursor)
			if err != nil {
				// Отмена во время прохода — часть остановки, не тревога.
				if ctx.Err() == nil {
					log.Warn("поздний переиздатель рендера", "err", err)
				}
				continue
			}
			// Wrapped на исчерпанном множестве — каждый тик стационара:
			// строкой журнала не отмечается (ревью T-259 P3-2), только
			// Debug; значимые события — публикации и стоящая очередь.
			if res.Published > 0 || res.Full {
				log.Info("поздний переиздатель рендера",
					"selected", res.Selected, "skipped", res.Skipped,
					"published", res.Published, "full", res.Full, "wrapped", res.Wrapped)
			} else if res.Wrapped {
				log.Debug("поздний переиздатель рендера: множество исчерпано, цикл замкнут")
			}
		}
	}
}

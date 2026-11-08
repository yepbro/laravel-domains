// Автомат состояния проверки схемы (T-266, ТЗ tasks/TZ-fsm.md §3 ФТ-12):
// единая точка правды о ДОПУСТИМЫХ комбинациях полей итога одной проверки
// (SchemeCheck, worker.go) и контентной части записи субдомен-результата
// (Record, worker.go). Состояние закодировано не отдельной колонкой, а
// сочетанием значений полей структур — центром формализации являются
// матрица легальных комбинаций и инварианты, а не таблица переходов
// (ТЗ-fsm §1: спецификация живёт в doc-комментарии пакета рядом с кодом).
// Писатели состояния — checkScheme и workerSkipSubdomain (worker.go),
// вариант-логика ProcessDomain + DetectAliases (httpfetch, T-054/T-055) и
// нормализация ContentSaver.Save (store.go, T-056); всё написанное ими
// обязано укладываться в матрицы ниже.
//
// Методы-валидаторы (ФТ-12) проверяют инварианты матриц на границах
// этапов:
//
//   - validateResult (SchemeCheck.validate + Record.validate по всем
//     субдоменам) — конец ProcessDomain, до передачи результата в
//     Options.OnResult (первое звено цепочки — ContentSaver, T-056):
//     нелегальная комбинация — баг построения результата, задача q:http
//     возвращается без XACK (ТЗ 3.4), мусор в БД не попадает;
//   - Record.validate — newRecordBatch (record.go, T-058) перед
//     транзакцией записи: записи после нормализации ContentSaver;
//     инварианты ContentNotStored ⇒ hash = nil и AliasNone ⇔ alias_id,
//     прежде проверявшиеся разрозненно (recordValidateRow), централизованы
//     здесь (ФТ-12).
//
// # Состояние проверки схемы (SchemeCheck, 7 полей)
//
// Вектор состояния: Available × Cached × ErrClass × DeadDial ×
// ContentHash≠nil × ContentStored × Truncated. Матрица ниже — ПЕРЕЧЕНЬ
// состояний писателей (все комбинации, которые пишет код); исполняемая
// часть спецификации — инварианты И1–И8 (SchemeCheck.validate):
// комбинации вне строк матрицы, не нарушающие инварианты (например,
// частичные записи прямых вызовов), валидатором допускаются.
//
//	| № | Доступна | Кэш | ErrClass | DeadDial | Хэш | Сохранено | Обрезано | Пишет |
//	|---|----------|-----|----------|----------|-----|-----------|----------|-------|
//	| С1 | да | — | ErrNone | — | ≠nil | да | — | checkScheme: ответ получен, ранний Put прошёл (T-147) |
//	| С2 | да | — | ErrNone | — | ≠nil | — | — | checkScheme: ответ получен, Put в паузе записи контента (ErrStoreFull/ErrTooLarge, ТЗ 8); той же формы итог веток storeErr (хэш-рассинхрон, сбой Put) — эпизод падает в ProcessDomain до валидации |
//	| С3 | да | — | ErrBody | — | ≠nil | — | да | checkScheme: тело обрезано потолком max_body_bytes (T-049), хэш — над обрезанными байтами |
//	| С4 | да | да | ErrNone | — | ≠nil | да | — | checkScheme: кэш-попадание страницы С1 (ТЗ 5.5 п. 6), FinalURL — стартовый URL (см. ниже) |
//	| С5 | да | да | ErrNone | — | ≠nil | — | — | checkScheme: кэш-попадание страницы С2 — пауза записи наследуется |
//	| С6 | да | да | ErrBody | — | ≠nil | — | да | checkScheme: кэш-попадание страницы С3 — класс и обрезка переносятся кэш-итогом |
//	| С7 | — | — | ErrNetwork | да | nil | — | — | checkScheme: connection refused стартового хоста, цепочка пуста (T-164) |
//	| С8 | — | — | ErrTimeout | да | nil | — | — | checkScheme: connect-таймаут стартового хоста, цепочка пуста (T-164) |
//	| С9 | — | — | ErrTimeout | — | nil | — | — | checkScheme: таймаут заголовков/общий/анти-тарпит/чтения тела; частичный итог (Status/Header/TLS/FinalURL/Redirects) может быть заполнен (§8) |
//	| С10 | — | — | ErrTLS | — | nil | — | — | checkScheme: сбой TLS-рукопожатия (InsecureSkipVerify не спасает от отказа установить соединение) |
//	| С11 | — | — | ErrNetwork | — | nil | — | — | checkScheme: прочие транспортные — обрыв, ошибка/резолв хоста редиректа (цепочка может быть непустой), unreachable/SSRF-фильтр диала (цепочка пуста); от С7/С8 отличает DeadDial=false — не отказ connect-фазы |
//	| С12 | — | — | ErrRedirects | — | nil | — | — | checkScheme: лимит max_redirects или цикл (T-048), Redirects — выполненная часть |
//	| С13 | — | — | ErrBody | — | nil | — | — | checkScheme: ошибка чтения тела после заголовков (битый Content-Encoding, zip-бомба — ErrDecompressTooLarge): протокол ответил, страницы нет |
//	| С14 | — | — | ErrCutoff | — | nil | — | — | workerSkipSubdomain: синтетика субдомена, пропущенного dead-host cutoff (T-164); записей не создаёт (§8) |
//
// Инварианты (нумерация — логическая; порядок проверки в validate()
// подобран так, чтобы каждая ветка была достижима: коды → кэш →
// недоступность → доступность → сохранение → cutoff → dead dial):
//
//	И1  Scheme и ErrClass — коды перечислений модели (§6.1, ErrorClass).
//	И2  DeadDial ⇒ недоступная, без кэша и хэша, с ПУСТОЙ цепочкой
//	    редиректов и классом connect-фазы: ErrNetwork (connection
//	    refused) или ErrTimeout (connect-таймаут) — сигнатура
//	    мёртвого хоста T-164 (httpfetch.IsConnectFailure).
//	И3  Cached ⇒ доступная с хэшем: кэш хранит только прочитанные
//	    страницы (workerURLCache.put — конец успешного checkScheme).
//	И4  Available ⇒ тело прочитано: ContentHash ≠ nil, FinalURL ≠ nil,
//	    SPA ≠ nil; ErrBody ⟺ Truncated — обрезка единственный ErrBody
//	    доступной проверки (ошибка чтения тела гасит Available, С13).
//	И5  ¬Available ⇒ контентной части нет: хэш nil, ¬Truncated,
//	    ¬ContentStored; частичные Status/Header/TLS/FinalURL — можно
//	    (данные для разбора, §8); Cached исключён И3.
//	И6  ContentStored ⇒ без обрезки (доступность и наличие хэша при
//	    ContentStored требуют И5/И4; обрезанное тело раннего Put не
//	    получает, T-049).
//	И7  ErrCutoff ⇒ ¬Available: cutoff — «проверка не выполнялась».
//	И8  ErrClass = ErrNone ⟺ ErrText = "" — ошибки всегда с текстом
//	    для разбора, успех — без.
//
// # Контентная часть записи (Record)
//
// Запись создаётся ТОЛЬКО доступной проверкой (DecideVariants, T-054:
// оба протокола молчат — записей нет), поэтому контентная часть записи —
// проекция состояний С1–С3 (кэш-попадания С4–С6 дают те же значения), а
// алиасная разметка — DetectAliases (T-055). Границы этапов несут две
// формы записи (до/после нормализации ContentSaver, store.go); матрица —
// перечень форм писателей, исполняемая часть — инварианты П1–П7
// (Record.validate, действуют на ОБЕИХ границах — пересечение форм,
// «—» = false):
//
//	| № | Граница | AliasKind | StoreContent | Хэш | ContentStored | ContentNotStored | Truncated | Пишет |
//	|---|---------|-----------|--------------|-----|---------------|------------------|-----------|-------|
//	| Р1 | обе | None | да | ≠nil | да | — | — | ProcessDomain: оригинал, ранний Put прошёл; ContentSaver не меняет |
//	| Р2 | до Save | None | да | ≠nil | — | — | — | ProcessDomain: оригинал, пауза записи контента → Save → Р6 |
//	| Р3 | до Save | None | да | ≠nil | — | — | да | ProcessDomain: оригинал, обрезка → Save → Р7 |
//	| Р4 | до Save | ≠None | — | ≠nil | да/— | — | — | DetectAliases: алиас — хэш своя копия проверки-источника, Save снимет → Р8 |
//	| Р5 | до Save | ≠None | — | ≠nil | — | — | да | DetectAliases: алиас обрезанной страницы → Save → Р8 |
//	| Р6 | после Save | None | да | nil | — | да | — | ContentSaver: контент не сохранён (пауза записи/лимит max_store_bytes), хэш снят |
//	| Р7 | после Save | None | да | nil | — | да | да | ContentSaver: обрезанное тело не хранится, хэш снят |
//	| Р8 | после Save | ≠None | — | nil | да/— | — | да/— | ContentSaver: алиас — контент у оригинала; Truncated/ContentStored — нетронутая копия src (Save их не меняет) |
//
// Инварианты:
//
//	П1  Variant/Scheme/AliasKind — коды перечислений модели (§6.1).
//	П2  (AliasKind = AliasNone) ⟺ (AliasSubdomainID = nil) — контракт
//	    T-055 (ФТ-12: прежде проверка recordValidateRow).
//	П3  StoreContent ⇒ AliasKind = AliasNone: контент класса хранится
//	    один раз у оригинала (T-055). Обратное не обязательно:
//	    DetectAliases одиночке без хэша даёт StoreContent=false (в
//	    реальном потоке воркера хэш есть всегда — И4; допущение — для
//	    частичных записей прямых вызовов newRecordBatch).
//	П4  ContentNotStored ⇒ хэш nil ∧ StoreContent ∧ ¬ContentStored —
//	    признак только оригинала, чей контент не сохранён; у алиасов
//	    не ставится (их контент — в записи-оригинале, T-055).
//	П5  Хэш либо nil, либо полный content.HashLen; оригинал
//	    (StoreContent) обязан нести хэш или признак ContentNotStored.
//	П6  Truncated ⇒ ¬ContentStored (обрезанное не сохраняется, T-049).
//	П7  ErrClass записи ∈ {ErrNone, ErrBody}, ErrBody ⟺ Truncated —
//	    проекция И4 доступной проверки-источника.
//
// # Осознанное искажение final_url при кэш-попадании
//
// Кэш-попадание (С4–С6) заполняет FinalURL СТАРТОВЫМ URLом проверки, а
// не итоговым URLом первоисточника (worker.go checkScheme, присвоение
// check.FinalURL = start). Ключ кэша — схема+хост+порт+путь БЕЗ query и
// фрагмента (workerCacheKey: счётчиковые параметры ?i=1 не должны
// разваливать дедупликацию одной страницы, ТЗ 5.5 п. 6), поэтому итоговый
// URL первоисточника с параметрами записывается своей без-запросной
// формой, а Redirects — синтетический единственный шаг со стартовым URL.
// Осознанное поведение, НЕ меняется (T-266, ТЗ-fsm §3 ФТ-12): хост и
// путь — критерии вариантов (T-054) и алиасов (T-055) — совпадают с
// первоисточником точно (входят в ключ), расхождение — только query
// стартовой формы; кэш не хранит цепочку чужой проверки.

package httpworker

import (
	"fmt"

	"crawler/internal/content"
	"crawler/internal/model"
)

// validate — централизованный валидатор состояния проверки схемы (ФТ-12,
// T-266): комбинация 7 полей состояния обязана удовлетворять инвариантам
// И1–И8 (матрица состояний писателей С1–С14 — doc-комментарий файла).
// Вызывается из validateResult (граница «перед ContentSaver»).
func (c *SchemeCheck) validate() error {
	// И1: коды перечислений — схема модели и класс ошибки этапа.
	if !c.Scheme.Valid() {
		return fmt.Errorf("проверка: схема %d", int16(c.Scheme))
	}
	if c.ErrClass < ErrNone || c.ErrClass > ErrCutoff {
		return fmt.Errorf("проверка %s: класс ошибки %d вне перечисления ErrorClass", c.Scheme, int16(c.ErrClass))
	}
	// И8: класс и текст ошибки согласованы — ошибка всегда с текстом
	// (разбор §8), успех без.
	if (c.ErrClass == ErrNone) != (c.ErrText == "") {
		return fmt.Errorf("проверка %s: класс %s при тексте ошибки %q", c.Scheme, c.ErrClass, c.ErrText)
	}
	// И3 (проверяется раньше И5/И4 — единственный владелец ветки Cached):
	// кэш-попадание — живая проверка с хэшем, кэш хранит только
	// прочитанные страницы.
	if c.Cached && (!c.Available || c.ContentHash == nil) {
		return fmt.Errorf("проверка %s: кэш-попадание без тела страницы (available=%v hash=%v)",
			c.Scheme, c.Available, c.ContentHash != nil)
	}
	// И5: контентная часть — только доступной проверки (ошибки загрузки
	// и чтения тела её не заполняют; Cached исключён И3).
	if !c.Available && (c.ContentHash != nil || c.Truncated || c.ContentStored) {
		return fmt.Errorf("проверка %s: недоступна, но несёт контентную часть (hash=%v truncated=%v stored=%v)",
			c.Scheme, c.ContentHash != nil, c.Truncated, c.ContentStored)
	}
	// И4: доступная проверка прочитала тело — хэш, итоговый URL и
	// SPA-вердикт есть; ErrBody ровно у обрезки.
	if c.Available {
		if c.ContentHash == nil || c.FinalURL == nil || c.SPA == nil {
			return fmt.Errorf("проверка %s: доступна без контентной части (hash=%v final_url=%v spa=%v)",
				c.Scheme, c.ContentHash != nil, c.FinalURL != nil, c.SPA != nil)
		}
		if (c.ErrClass == ErrBody) != c.Truncated {
			return fmt.Errorf("проверка %s: класс %s при Truncated=%v (ErrBody доступной проверки — только обрезка)",
				c.Scheme, c.ErrClass, c.Truncated)
		}
	}
	// И6: сохранённое тело — без обрезки (недоступность и отсутствие
	// хэша при ContentStored отсекают И5/И4; обрезанное тело раннего
	// Put не получает, T-049).
	if c.ContentStored && c.Truncated {
		return fmt.Errorf("проверка %s: ContentStored при обрезанном теле (T-049)", c.Scheme)
	}
	// И7: cutoff — синтетика пропущенного субдомена (workerSkipSubdomain),
	// доступной проверкой не бывает.
	if c.ErrClass == ErrCutoff && c.Available {
		return fmt.Errorf("проверка %s: ErrCutoff у доступной проверки", c.Scheme)
	}
	// И2: мёртвый диал — недоступная проверка с пустой цепочкой и классом
	// отказа connect-фазы (T-164).
	if c.DeadDial && (c.Available || c.Cached || c.ContentHash != nil || len(c.Redirects) > 0 ||
		(c.ErrClass != ErrNetwork && c.ErrClass != ErrTimeout)) {
		return fmt.Errorf("проверка %s: DeadDial без сигнатуры мёртвого connect (класс %s, шагов цепочки %d)",
			c.Scheme, c.ErrClass, len(c.Redirects))
	}
	return nil
}

// validate — централизованный валидатор контентной части записи (ФТ-12,
// T-266): инварианты П1–П7 действуют на обеих границах (матрица форм
// писателей Р1–Р8 — doc-комментарий файла). Вызывается из validateResult
// (записи до ContentSaver) и newRecordBatch перед транзакцией записи
// (после нормализации).
func (r *Record) validate() error {
	// П1: коды модели (§6.1).
	if !r.Variant.Valid() {
		return fmt.Errorf("субдомен %d: вариант %d", r.SubdomainID, int16(r.Variant))
	}
	if !r.Scheme.Valid() {
		return fmt.Errorf("субдомен %d: схема %d", r.SubdomainID, int16(r.Scheme))
	}
	if !r.AliasKind.Valid() {
		return fmt.Errorf("субдомен %d: вид алиаса %d", r.SubdomainID, int16(r.AliasKind))
	}
	// П2: связь алиаса самосогласована (контракт T-055).
	if (r.AliasKind == model.AliasNone) != (r.AliasSubdomainID == nil) {
		return fmt.Errorf("субдомен %d: alias_kind %d ⟺ alias_subdomain_id (контракт T-055)",
			r.SubdomainID, int16(r.AliasKind))
	}
	// П3: контент класса хранится один раз у оригинала — StoreContent
	// только у независимой записи.
	if r.StoreContent && r.AliasKind != model.AliasNone {
		return fmt.Errorf("субдомен %d: StoreContent у алиаса (контент класса — у оригинала, T-055)", r.SubdomainID)
	}
	// П4: признак «не сохранён» — только оригинала, только без хэша и
	// без сохранённого тела (у алиасов не ставится).
	if r.ContentNotStored && (r.ContentHash != nil || !r.StoreContent || r.ContentStored) {
		return fmt.Errorf("субдомен %d: ContentNotStored при живом content_hash/алиасе/сохранённом теле (контракт T-056)",
			r.SubdomainID)
	}
	// П5: хэш — полный content.Hash; оригинал несёт хэш или признак его
	// отсутствия (типизирован content.Hash — проверка длины страховка
	// прямых вызовов).
	if r.ContentHash != nil && len(*r.ContentHash) != content.HashLen {
		return fmt.Errorf("субдомен %d: content_hash %d байт (нужно %d)",
			r.SubdomainID, len(*r.ContentHash), content.HashLen)
	}
	if r.StoreContent && r.ContentHash == nil && !r.ContentNotStored {
		return fmt.Errorf("субдомен %d: оригинал без контента обязан нести хэш или ContentNotStored (контракт T-056)",
			r.SubdomainID)
	}
	// П6: обрезанное тело не сохраняется (T-049).
	if r.Truncated && r.ContentStored {
		return fmt.Errorf("субдомен %d: Truncated при ContentStored (обрезанное не сохраняется, T-049)", r.SubdomainID)
	}
	// П7: запись создаётся только доступной проверкой — класс ErrNone или
	// ErrBody (обрезка), прочие классы живут в проверках, не в записях.
	if r.ErrClass != ErrNone && r.ErrClass != ErrBody {
		return fmt.Errorf("субдомен %d: класс ошибки записи %s (запись создаётся только доступной проверкой)",
			r.SubdomainID, r.ErrClass)
	}
	if (r.ErrClass == ErrBody) != r.Truncated {
		return fmt.Errorf("субдомен %d: класс %s при Truncated=%v (ErrBody записи — только обрезка)",
			r.SubdomainID, r.ErrClass, r.Truncated)
	}
	return nil
}

// validateResult — граница этапа «перед ContentSaver» (ФТ-12, T-266):
// все проверки и записи результата домена проходят валидаторы автомата.
// Вызывается из ProcessDomain (worker.go) до передачи результата в
// Options.OnResult; отдельная функция — для тестируемости границы
// (внутри ProcessDomain нелегальное состояние построить нечем: все поля
// собирают checkScheme/DetectAliases легально).
func validateResult(res *DomainResult) error {
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for _, c := range []*SchemeCheck{&sub.HTTPS, &sub.HTTP} {
			if err := c.validate(); err != nil {
				return fmt.Errorf("httpworker: субдомен %s: %w", sub.Hostname, err)
			}
		}
		for j := range sub.Records {
			if err := sub.Records[j].validate(); err != nil {
				return fmt.Errorf("httpworker: субдомен %s: %w", sub.Hostname, err)
			}
		}
	}
	return nil
}

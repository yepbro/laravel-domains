// Package model — типизация домена в Go, зеркалящая схему PostgreSQL
// (миграция 000001): структуры строк таблиц, перечисления (smallint-коды
// ТЗ 6.1/6.2), битовые признаки Flags и маска субдоменов DNSMask.
//
// Пакет сознательно не содержит логики проверки: переходы статусов и
// состояний выполняют планировщик и воркеры (ТЗ 7), форматы сообщений
// очередей вводит этап 2 (T-024) — здесь только значения, их разбор,
// валидация и битовые операции над ними.
//
// Типы Go подобраны под колонки (ТЗ 6: «размеры полей минимальные»):
//
//	smallint   → int16 или именованный тип на его основе (Status, …);
//	int        → int32;   bigint → int64;  real → float32;
//	text       → string;  bytea  → []byte; timestamptz/date → time.Time;
//	jsonb      → []byte — сырой JSON; конкретные структуры вводят
//	             HTTP-этап (T-058) и анализатор (T-069).
//
// NULL-колонки — указатели (bytea/jsonb достаточно nil []byte). Теги db
// повторяют имена колонок из миграции в том же порядке, поэтому структуры
// пригодны для pgx.RowToStructByName; соответствие тегов схеме закреплено
// тестом TestStructColumnsMatchSchema.
package model

import "time"

// Zone — строка zones: справочник зон (TLD + тестовые зоны allowlist)
// со счётчиками прогресса (обновляет monitor, ТЗ 5.9).
type Zone struct {
	ID             int16      `db:"id"`
	Name           string     `db:"name"`         // punycode
	NameUnicode    *string    `db:"name_unicode"` // только отображение (ТЗ 5.2 п. 7)
	IsActive       bool       `db:"is_active"`
	DomainsTotal   int64      `db:"domains_total"`
	DomainsChecked int64      `db:"domains_checked"`
	UpdatedAt      *time.Time `db:"updated_at"`
}

// Subdomain — строка subdomains: справочник проверяемых субдоменов.
// ID — номер бита в DNSMask (0..62, ТЗ 5.10), Label "" — апекс.
// Добавление субдомена не требует миграции данных (ТЗ 5.10).
type Subdomain struct {
	ID      int16  `db:"id"`
	Label   string `db:"label"`
	Enabled bool   `db:"enabled"`
}

// Domain — строка domains: домен зоны и состояние его проверки.
// Label — левая часть имени без зоны, в punycode (ТЗ 5.2): внутри
// системы везде ASCII-форма.
type Domain struct {
	ZoneID        int16      `db:"zone_id"`
	ID            int64      `db:"id"`
	Label         string     `db:"label"`
	Status        Status     `db:"status"`          // ТЗ 6.2
	CheckState    CheckState `db:"check_state"`     // 0 pending, 1 queued, 2 done
	Flags         Flags      `db:"flags"`           // бит 0 is_idn, бит 1 render_pending
	DNSMask       DNSMask    `db:"dns_mask"`        // резолвящиеся субдомены
	QueuedAt      *time.Time `db:"queued_at"`       // lease постановки в очередь
	LastCheckedAt *time.Time `db:"last_checked_at"` // date; индексируется частично (domains_recheck_idx, T-104)
	// TempAttempts — число назначенных подряд повторных проверок
	// temp_error (T-031, миграция 000005): наращивает планировщик при
	// назначении повтора, сбрасывается при итоговом статусе и успешной
	// проверке (ТЗ 5.3).
	TempAttempts int16 `db:"temp_attempts"`
}

// SubdomainResult — строка subdomain_results: результат проверки одного
// (домен, субдомен, вариант). Первичный ключ — первые четыре колонки;
// перепроверка домена перезаписывает строки с тем же ключом (ТЗ 3.5).
type SubdomainResult struct {
	ZoneID           int16     `db:"zone_id"`
	DomainID         int64     `db:"domain_id"`
	SubdomainID      int16     `db:"subdomain_id"`
	Variant          Variant   `db:"variant"` // 0 основной, 1 http (если отличается)
	Scheme           Scheme    `db:"scheme"`  // 1 http, 2 https
	HTTPStatus       *int16    `db:"http_status"`
	FinalURL         *string   `db:"final_url"` // страница в конце цепочки редиректов
	AliasKind        AliasKind `db:"alias_kind"`
	AliasSubdomainID *int16    `db:"alias_subdomain_id"`
	Addrs            []byte    `db:"addrs"` // упакованные A/AAAA (формат — DNS-этап)
	CNAME            *string   `db:"cname"`
	TLS              []byte    `db:"tls"`       // jsonb: сертификат, издатель, срок, SAN
	Redirects        []byte    `db:"redirects"` // jsonb: цепочка редиректов
	Headers          []byte    `db:"headers"`   // jsonb: заголовки ответа
	Cookies          []byte    `db:"cookies"`   // jsonb
	Rendered         bool      `db:"rendered"`
	ContentHash      []byte    `db:"content_hash"` // 16 байт, ссылка на contents
	Error            *int16    `db:"error"`
	ResponseMs       *int32    `db:"response_ms"`
	CheckedAt        time.Time `db:"checked_at"`
	RulesVersion     *int32    `db:"rules_version"`
}

// Content — строка contents: метаданные контента в пак-файлах хранилища
// (ТЗ 5.7). Hash — 128-битная дедупликация; RefCount — число ссылающихся
// результатов.
type Content struct {
	Hash     []byte `db:"hash"` // 16 байт
	PackID   int32  `db:"pack_id"`
	Off      int64  `db:"off"`
	Len      int32  `db:"len"`
	RawLen   int32  `db:"raw_len"`
	RefCount int32  `db:"refcount"`
}

// LaravelDomain — строка laravel_domains: агрегат признаков Laravel по
// домену (ТЗ 5.8). FoundOnMask — те же биты субдоменов, что и DNSMask.
type LaravelDomain struct {
	ZoneID       int16     `db:"zone_id"`
	DomainID     int64     `db:"domain_id"`
	Score        float32   `db:"score"`
	VerMin       *string   `db:"ver_min"`
	VerMax       *string   `db:"ver_max"`
	FoundOnMask  DNSMask   `db:"found_on_mask"` // субдомены, где найден Laravel
	RulesVersion int32     `db:"rules_version"`
	UpdatedAt    time.Time `db:"updated_at"`
}

// LaravelFinding — строка laravel_findings: срабатывания правил по одному
// результату (домен, субдомен, вариант); пишутся при любом срабатывании
// независимо от порога (ТЗ 5.8).
type LaravelFinding struct {
	ZoneID       int16   `db:"zone_id"`
	DomainID     int64   `db:"domain_id"`
	SubdomainID  int16   `db:"subdomain_id"`
	Variant      Variant `db:"variant"`
	Score        float32 `db:"score"`
	VerMin       *string `db:"ver_min"`
	VerMax       *string `db:"ver_max"`
	Signals      []byte  `db:"signals"` // jsonb: [{rule, weight, evidence}, …]
	IsLaravel    bool    `db:"is_laravel"`
	RulesVersion int32   `db:"rules_version"` // хэш набора (internal/rules)
}

// IngestFile — строка ingest_files: журнал обработанных входных файлов,
// идемпотентность по sha256 (ТЗ 5.1). Счётчики — итог применения файла.
type IngestFile struct {
	ID          int64      `db:"id"`
	ZoneID      *int16     `db:"zone_id"`
	Action      *Action    `db:"action"` // 1 add, 2 delete
	Path        *string    `db:"path"`
	SHA256      []byte     `db:"sha256"`
	LinesTotal  *int64     `db:"lines_total"`
	Added       *int64     `db:"added"`
	Existed     *int64     `db:"existed"`
	Deleted     *int64     `db:"deleted"`
	Rejected    *int64     `db:"rejected"`
	ProcessedAt *time.Time `db:"processed_at"`
}

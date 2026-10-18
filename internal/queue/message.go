// Форматы сообщений стримов (ТЗ 4.2: «сообщение содержит только
// идентификаторы и минимально необходимые данные (имя домена)»).
//
// Запись стрима хранит единственное поле data — JSON-представление
// сообщения (запись/чтение — redisq.go). JSON, а не побитовая упаковка:
// сообщения крошечные (идентификаторы и имя домена, десятки байт),
// выигрыш компактного формата — проценты, а читаемость в redis-cli
// (XRANGE показывает готовый JSON) и свобода эволюции важнее: добавление
// поля не ломает читателей — неизвестные поля JSON игнорируются, поэтому
// T-025 введёт счётчик попыток доставки тем же форматом.
//
// Первое поле каждого сообщения — task_id, уникальный идентификатор
// задачи (NewTaskID). Он переживает запись стрима: при исчерпании
// попыток сообщение переносится в q:dead с тем же task_id (T-025), что
// связывает исходную задачу, повторные доставки и ручной разбор.

package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"

	"crawler/internal/model"
)

// Максимумы имён по ТЗ 5.2 (п. 4): метка ≤ 63, полное имя ≤ 253 октета
// в ASCII-форме. Очередь не нормализует имена повторно (это сделано
// на входе — internal/idn), а только отвергает заведомо невозможные.
const (
	maxLabelLen = 63
	maxNameLen  = 253
)

// Message — сообщение одного из стримов очереди: DNSBatch (q:dns),
// HTTPTask (q:http), Result (q:render, q:analyze), Dead (q:dead).
// Интерфейс запечатан неэкспортируемым методом validate: форматы стримов
// вводит только пакет queue, сторонние типы публиковать нельзя.
//
// Имена — ASCII-форма (punycode, ТЗ 5.2): в очереди, как и во всей
// системе, ходят только ASCII-имена.
type Message interface {
	// Streams возвращает стримы, в которые допустима публикация
	// сообщения (ТЗ 4.2). У Result их два: q:render и q:analyze
	// используют один формат «один результат».
	Streams() []string

	// validate проверяет минимальную допустимость значений перед
	// публикацией и после декодирования.
	validate() error
}

// DNSDomain — домен пачки q:dns: идентификаторы строки domains и имя
// (ТЗ 4.2: «пачка доменов (id + имя)»). dns-worker делает запросы по
// имени, результаты пишет по идентификаторам; zone_id включён как
// идентификатор записи — он нужен партиционированным таблицам результатов
// и избавляет воркер от обратного чтения domains.
type DNSDomain struct {
	ZoneID   int16  `json:"zone_id"`
	DomainID int64  `json:"domain_id"`
	Name     string `json:"name"` // полное имя, punycode
}

// DNSBatch — сообщение q:dns: пачка доменов на DNS-проверку (ТЗ 4.2,
// §5.3 — планировщик публикует пачками; размер пачки — scheduler.batch_size).
type DNSBatch struct {
	TaskID  string      `json:"task_id"`
	Domains []DNSDomain `json:"domains"`
}

// Streams — стрим пачки DNS-задач.
func (DNSBatch) Streams() []string { return []string{StreamDNS} }

func (b DNSBatch) validate() error {
	if len(b.Domains) == 0 {
		return fmt.Errorf("queue: q:dns: пустая пачка доменов")
	}
	for i, d := range b.Domains {
		if d.ZoneID <= 0 {
			return fmt.Errorf("queue: q:dns: домен №%d: zone_id %d ≤ 0", i+1, d.ZoneID)
		}
		if d.DomainID <= 0 {
			return fmt.Errorf("queue: q:dns: домен №%d: domain_id %d ≤ 0", i+1, d.DomainID)
		}
		if err := checkName(d.Name); err != nil {
			return fmt.Errorf("queue: q:dns: домен №%d: %w", i+1, err)
		}
	}
	return nil
}

// HTTPSubdomain — DNS-живой субдомен задачи q:http: номер бита dns_mask
// (subdomains.id), метка ("" — апекс домена) и адреса с CNAME из
// DNS-этапа.
type HTTPSubdomain struct {
	SubdomainID int16    `json:"subdomain_id"`
	Label       string   `json:"label"`           // "" — апекс
	Addrs       []string `json:"addrs"`           // A/AAAA, уже резолвлены
	CNAME       string   `json:"cname,omitempty"` // если субдомен — CNAME
}

// HTTPTask — сообщение q:http: один домен со списком DNS-живых
// субдоменов (ТЗ 4.2). Адреса и CNAME включены по ТЗ 5.4 (п. 4: «адреса
// и CNAME передаются в задачу q:http») — http-worker подключается к
// заранее резолвленным IP (ТЗ 5.5, п. 2) и не делает повторных DNS-
// запросов для субдоменов задачи.
type HTTPTask struct {
	TaskID     string          `json:"task_id"`
	ZoneID     int16           `json:"zone_id"`
	DomainID   int64           `json:"domain_id"`
	Name       string          `json:"name"` // полное имя домена, punycode
	Subdomains []HTTPSubdomain `json:"subdomains"`
}

// Streams — стрим HTTP-задач.
func (HTTPTask) Streams() []string { return []string{StreamHTTP} }

func (t HTTPTask) validate() error {
	if t.ZoneID <= 0 {
		return fmt.Errorf("queue: q:http: zone_id %d ≤ 0", t.ZoneID)
	}
	if t.DomainID <= 0 {
		return fmt.Errorf("queue: q:http: domain_id %d ≤ 0", t.DomainID)
	}
	if err := checkName(t.Name); err != nil {
		return fmt.Errorf("queue: q:http: %w", err)
	}
	if len(t.Subdomains) == 0 {
		return fmt.Errorf("queue: q:http: нет DNS-живых субдоменов (задача не публикуется, ТЗ 5.4 п. 5)")
	}
	for i, s := range t.Subdomains {
		if err := model.ValidateSubdomainID(int(s.SubdomainID)); err != nil {
			return fmt.Errorf("queue: q:http: субдомен №%d: %w", i+1, err)
		}
		if len(s.Label) > maxLabelLen || strings.ContainsAny(s.Label, ". ") {
			return fmt.Errorf("queue: q:http: субдомен №%d: метка %q — одна DNS-метка до %d октетов", i+1, s.Label, maxLabelLen)
		}
		if len(s.Addrs) == 0 {
			return fmt.Errorf("queue: q:http: субдомен №%d: нет адресов", i+1)
		}
		for _, a := range s.Addrs {
			if _, err := netip.ParseAddr(a); err != nil {
				return fmt.Errorf("queue: q:http: субдомен №%d: адрес %q: %w", i+1, a, err)
			}
		}
		if len(s.CNAME) > maxNameLen {
			return fmt.Errorf("queue: q:http: субдомен №%d: CNAME длиннее %d октетов", i+1, maxNameLen)
		}
	}
	return nil
}

// Result — сообщение q:render и q:analyze: один результат — ключ строки
// subdomain_results (домен, субдомен, вариант; ТЗ 4.2, §6.1). Сам
// результат остаётся в БД, сообщение только указывает, какую запись
// обрабатывать (рендер берёт итоговый URL, анализатор — контент по ключу);
// имя домена — минимальное данное для логов и ручного разбора (ТЗ 4.2).
type Result struct {
	TaskID      string        `json:"task_id"`
	ZoneID      int16         `json:"zone_id"`
	DomainID    int64         `json:"domain_id"`
	SubdomainID int16         `json:"subdomain_id"`
	Variant     model.Variant `json:"variant"`        // 0 основной, 1 http (ТЗ 6.1)
	Name        string        `json:"name,omitempty"` // полное имя, punycode
	// Fresh — строка записана новым эпизодом HTTP-проверки (T-118):
	// публикация HTTP/рендер-этапом (в той же транзакции эпизода) — true,
	// переанализ по сохранённому контенту (ReanalyzeLoop, T-074) — false.
	// Читает анализатор: история детекции laravel_history (appeared/lost)
	// пишется только по свежим эпизодам — смена правил не факт о сайте.
	// omitempty: записи до T-118 читаются как false (переанализ), старые
	// и новые форматы совместимы без миграции стрима.
	Fresh bool `json:"fresh,omitempty"`
}

// Streams — стримы результата: формат один, рендер и анализ публикуют
// одинаковое сообщение.
func (Result) Streams() []string { return []string{StreamRender, StreamAnalyze} }

func (r Result) validate() error {
	if r.ZoneID <= 0 {
		return fmt.Errorf("queue: результат: zone_id %d ≤ 0", r.ZoneID)
	}
	if r.DomainID <= 0 {
		return fmt.Errorf("queue: результат: domain_id %d ≤ 0", r.DomainID)
	}
	if err := model.ValidateSubdomainID(int(r.SubdomainID)); err != nil {
		return fmt.Errorf("queue: результат: %w", err)
	}
	if !r.Variant.Valid() {
		return fmt.Errorf("queue: результат: вариант %d (0 основной, 1 http)", r.Variant)
	}
	if len(r.Name) > maxNameLen {
		return fmt.Errorf("queue: результат: имя длиннее %d октетов", maxNameLen)
	}
	return nil
}

// Dead — сообщение q:dead: задача, исчерпавшая число попыток (ТЗ 4.2),
// для ручного разбора. Продюсер — T-025 (счётчик попыток и перевод
// в q:dead); формат зафиксирован здесь заранее, чтобы эволюция доставки
// не меняла контракт стрима. Записи несут идентификаторы предмета
// задачи, а не исходное сообщение (T-105): payload пачек q:dns до
// ~250 КБ при потолке 10 000 записей давал worst-case ~2,5 ГБ против
// бюджета Redis ≤ 0,5 ГБ (ТЗ 3.2, docs/redis.md §3), а ручному разбору
// хватает идентификаторов (docs/runbooks.md §2: домен — name/domain_id,
// ключ строки — zone/domain/subdomain/variant; домены пачки q:dns
// переиздаст lease-проход планировщика). Payload сохранено только для
// чтения старых записей (до T-105) — новые его не публикуют.
type Dead struct {
	TaskID   string `json:"task_id"`          // task_id исходной задачи
	Stream   string `json:"stream"`           // стрим-источник
	EntryID  string `json:"entry_id"`         // запись-источник
	Attempts int    `json:"attempts"`         // число попыток доставки
	Reason   string `json:"reason,omitempty"` // причина исчерпания попыток

	// Идентификаторы предмета задачи (T-105) — выжимка исходного
	// сообщения (deadSubjectOf). Опциональны: перенос в q:dead не обязан
	// падать на тело, не разбирающееся как сообщение стрима.
	ZoneID      int16         `json:"zone_id,omitempty"`      // штучные задачи: зона домена
	DomainID    int64         `json:"domain_id,omitempty"`    // штучные задачи: домен
	SubdomainID int16         `json:"subdomain_id,omitempty"` // q:render/q:analyze: ключ строки
	Variant     model.Variant `json:"variant,omitempty"`      // q:render/q:analyze: 0 опускается (основной)
	Name        string        `json:"name,omitempty"`         // имя домена, punycode
	Domains     int           `json:"domains,omitempty"`      // q:dns: размер пачки

	// Payload — исходное сообщение записи до T-105; новыми записями не
	// публикуется, читается для истории разбора и разбором игнорируется.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Streams — стрим мёртвых задач.
func (Dead) Streams() []string { return []string{StreamDead} }

func (d Dead) validate() error {
	if d.TaskID == "" {
		return fmt.Errorf("queue: q:dead: пустой task_id")
	}
	switch d.Stream {
	case StreamDNS, StreamHTTP, StreamRender, StreamAnalyze:
	default:
		return fmt.Errorf("queue: q:dead: стрим-источник %q не является рабочим стримом задач", d.Stream)
	}
	if d.EntryID == "" {
		return fmt.Errorf("queue: q:dead: пустой entry_id")
	}
	if d.Attempts < 1 {
		return fmt.Errorf("queue: q:dead: attempts %d < 1", d.Attempts)
	}
	// Идентификаторы предмета (T-105) опциональны (deadSubjectOf —
	// best-effort), заданные обязаны быть допустимыми.
	if d.ZoneID < 0 || d.DomainID < 0 {
		return fmt.Errorf("queue: q:dead: идентификаторы домена %d/%d отрицательны", d.ZoneID, d.DomainID)
	}
	if d.Domains < 0 {
		return fmt.Errorf("queue: q:dead: размер пачки %d < 0", d.Domains)
	}
	if len(d.Name) > maxNameLen {
		return fmt.Errorf("queue: q:dead: имя длиннее %d октетов", maxNameLen)
	}
	if !d.Variant.Valid() {
		return fmt.Errorf("queue: q:dead: вариант %d (0 основной, 1 http)", d.Variant)
	}
	if err := model.ValidateSubdomainID(int(d.SubdomainID)); err != nil {
		return fmt.Errorf("queue: q:dead: %w", err)
	}
	if len(d.Payload) > 0 && !json.Valid(d.Payload) {
		return fmt.Errorf("queue: q:dead: payload — исходное сообщение старой записи (JSON), задано %d байт", len(d.Payload))
	}
	return nil
}

// deadSubjectOf выжимает из тела исходного сообщения идентификаторы
// предмета задачи в запись Dead (T-105): q:dns — размер пачки (список
// доменов не переносится — до ~250 КБ на запись, см. Dead), q:http —
// домен, q:render/q:analyze — ключ строки subdomain_results.
// Best-effort: тело, не разбирающееся как сообщение стрима, оставляет
// поля нулевыми; разобравшееся, но чужое (недопустимые значения) bury
// откатывает к записи без идентификаторов — перенос в q:dead не падает
// и не застревает ни на каком теле с task_id.
func deadSubjectOf(d *Dead, stream string, payload []byte) {
	switch stream {
	case StreamDNS:
		// Элементы пачки не читаются — только их число: []struct{}
		// разбирает массив, не аллоцируя имена и идентификаторы.
		var m struct {
			Domains []struct{} `json:"domains"`
		}
		if json.Unmarshal(payload, &m) == nil {
			d.Domains = len(m.Domains)
		}
	case StreamHTTP:
		var m HTTPTask
		if json.Unmarshal(payload, &m) == nil {
			d.ZoneID, d.DomainID, d.Name = m.ZoneID, m.DomainID, m.Name
		}
	case StreamRender, StreamAnalyze:
		var m Result
		if json.Unmarshal(payload, &m) == nil {
			d.ZoneID, d.DomainID = m.ZoneID, m.DomainID
			d.SubdomainID, d.Variant, d.Name = m.SubdomainID, m.Variant, m.Name
		}
	}
}

// NewTaskID генерирует уникальный идентификатор задачи: 16 случайных
// байт (crypto/rand) в hex — 128 бит случайности, коллизии практически
// исключены. Идентификатор живёт в сообщении (не в записи стрима):
// перенос задачи в q:dead (T-025) сохраняет его для связи с исходной
// задачей и логами.
func NewTaskID() string {
	var b [16]byte
	// С Go 1.24 crypto/rand.Read не возвращает ошибок (заполняет буфер
	// целиком или паникует).
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// marshalMessage сериализует сообщение в JSON-поле data. Пустой TaskID
// заменяется новым (копия сообщения — изменение не видно вызывающему;
// идентификатор остаётся внутри записи очереди). Нужен идентификатор
// заранее — заполните поле NewTaskID() до публикации.
func marshalMessage(msg Message) ([]byte, error) {
	switch m := msg.(type) {
	case DNSBatch:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	case *DNSBatch:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	case HTTPTask:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	case *HTTPTask:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	case Result:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	case *Result:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	case Dead:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	case *Dead:
		if m.TaskID == "" {
			m.TaskID = NewTaskID()
		}
		return json.Marshal(m)
	default:
		return nil, fmt.Errorf("queue: неизвестный тип сообщения %T (форматы вводит только пакет queue)", msg)
	}
}

// Decode декодирует полезную нагрузку доставки в сообщение типа T,
// проверяя стрим доставки и допустимость значений:
//
//	batch, err := queue.Decode[queue.DNSBatch](d)
//
// Неизвестные поля JSON игнорируются — новые поля будущих версий
// (счётчик попыток T-025) не ломают чтение.
func Decode[T Message](d Delivery) (T, error) {
	var m T
	if len(d.Payload) == 0 {
		return m, fmt.Errorf("queue: %s: запись %s без поля data", d.Stream, d.ID)
	}
	if err := json.Unmarshal(d.Payload, &m); err != nil {
		return m, fmt.Errorf("queue: %s: запись %s: %w", d.Stream, d.ID, err)
	}
	if !streamAllowed(m, d.Stream) {
		return m, fmt.Errorf("queue: %s: запись %s: сообщение %T не относится к этому стриму (допустимы %v)",
			d.Stream, d.ID, m, m.Streams())
	}
	if err := m.validate(); err != nil {
		return m, fmt.Errorf("queue: %s: запись %s: %w", d.Stream, d.ID, err)
	}
	return m, nil
}

// streamAllowed сообщает, что сообщение msg может лежать в стриме stream.
func streamAllowed(msg Message, stream string) bool {
	for _, s := range msg.Streams() {
		if s == stream {
			return true
		}
	}
	return false
}

// checkName проверяет полное имя домена: непустое и в пределах лимита
// ТЗ 5.2 (п. 4). Имя ожидается уже нормализованным (punycode); глубокая
// проверка синтаксиса — территория internal/idn на входе в систему.
func checkName(name string) error {
	if name == "" {
		return fmt.Errorf("пустое имя домена")
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("имя длиннее %d октетов", maxNameLen)
	}
	return nil
}

// Тесты форматов сообщений стримов (T-024, ТЗ 4.2): wire-формат
// зафиксирован точными JSON-строками, roundtrip — через Decode, кривые
// данные — ошибками декодирования/валидации.

package queue

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"crawler/internal/model"
)

func TestMessageStreams(t *testing.T) {
	cases := []struct {
		msg  Message
		want []string
	}{
		{DNSBatch{}, []string{StreamDNS}},
		{HTTPTask{}, []string{StreamHTTP}},
		{Result{}, []string{StreamRender, StreamAnalyze}},
		{Dead{}, []string{StreamDead}},
	}
	for _, c := range cases {
		if got, want := c.msg.Streams(), c.want; !reflect.DeepEqual(got, want) {
			t.Errorf("%T.Streams() = %v, хочу %v", c.msg, got, want)
		}
	}
}

// TestMessageWireFormat фиксирует точный JSON-формат сообщений всех
// стримов: имена и порядок полей закреплены, эволюция — только добавлением
// полей (неизвестные поля читатели игнорируют).
func TestMessageWireFormat(t *testing.T) {
	cases := []struct {
		name string
		msg  Message
		want string
	}{
		{
			"q:dns",
			DNSBatch{
				TaskID: "t1",
				Domains: []DNSDomain{
					{ZoneID: 3, DomainID: 42, Name: "example.com"},
					{ZoneID: 3, DomainID: 43, Name: "xn--80ak6aa92e.com"},
				},
			},
			`{"task_id":"t1","domains":[{"zone_id":3,"domain_id":42,"name":"example.com"},` +
				`{"zone_id":3,"domain_id":43,"name":"xn--80ak6aa92e.com"}]}`,
		},
		{
			"q:http",
			HTTPTask{
				TaskID:   "t2",
				ZoneID:   3,
				DomainID: 42,
				Name:     "example.com",
				Subdomains: []HTTPSubdomain{
					{SubdomainID: 0, Label: "", Addrs: []string{"93.184.216.34"}},
					{SubdomainID: 1, Label: "www", Addrs: []string{"93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"}, CNAME: "example.com"},
				},
			},
			`{"task_id":"t2","zone_id":3,"domain_id":42,"name":"example.com","subdomains":[` +
				`{"subdomain_id":0,"label":"","addrs":["93.184.216.34"]},` +
				`{"subdomain_id":1,"label":"www","addrs":["93.184.216.34","2606:2800:220:1:248:1893:25c8:1946"],"cname":"example.com"}]}`,
		},
		{
			"q:render/q:analyze",
			Result{TaskID: "t3", ZoneID: 3, DomainID: 42, SubdomainID: 1, Variant: model.VariantHTTP, Name: "www.example.com"},
			`{"task_id":"t3","zone_id":3,"domain_id":42,"subdomain_id":1,"variant":1,"name":"www.example.com"}`,
		},
		{
			"результат без имени",
			Result{TaskID: "t4", ZoneID: 3, DomainID: 42, SubdomainID: 0, Variant: model.VariantMain},
			`{"task_id":"t4","zone_id":3,"domain_id":42,"subdomain_id":0,"variant":0}`,
		},
		{
			"q:dead пачки",
			Dead{
				TaskID:   "t1",
				Stream:   StreamDNS,
				EntryID:  "1718-0",
				Attempts: 3,
				Reason:   "исчерпан лимит попыток 3: доставок без подтверждения — 4",
				Domains:  5000,
			},
			`{"task_id":"t1","stream":"q:dns","entry_id":"1718-0","attempts":3,` +
				`"reason":"исчерпан лимит попыток 3: доставок без подтверждения — 4","domains":5000}`,
		},
		{
			"q:dead штучной задачи",
			Dead{
				TaskID:      "t2",
				Stream:      StreamRender,
				EntryID:     "1719-0",
				Attempts:    4,
				ZoneID:      3,
				DomainID:    42,
				SubdomainID: 1,
				Variant:     model.VariantHTTP,
				Name:        "www.example.com",
			},
			`{"task_id":"t2","stream":"q:render","entry_id":"1719-0","attempts":4,` +
				`"zone_id":3,"domain_id":42,"subdomain_id":1,"variant":1,"name":"www.example.com"}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := marshalMessage(c.msg)
			if err != nil {
				t.Fatalf("marshalMessage(%T): %v", c.msg, err)
			}
			if string(got) != c.want {
				t.Errorf("формат %T:\n получил  %s\n хочу     %s", c.msg, got, c.want)
			}
		})
	}
}

// TestMessageRoundtrip — marshal → Decode по каждому стриму сообщения
// (Result читается и из q:render, и из q:analyze).
func TestMessageRoundtrip(t *testing.T) {
	batch := DNSBatch{
		TaskID: "rt1",
		Domains: []DNSDomain{
			{ZoneID: 7, DomainID: 1000000, Name: "xn--80ak6aa92e.com"},
			{ZoneID: 7, DomainID: 1000001, Name: "xn--e1afmkfd.xn--p1ai"},
		},
	}
	task := HTTPTask{
		TaskID: "rt2", ZoneID: 7, DomainID: 1000000, Name: "xn--e1afmkfd.xn--p1ai",
		Subdomains: []HTTPSubdomain{
			{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.1"}},
			{SubdomainID: 2, Label: "admin", Addrs: []string{"192.0.2.2"}, CNAME: "xn--e1afmkfd.xn--p1ai"},
		},
	}
	result := Result{TaskID: "rt3", ZoneID: 7, DomainID: 1000000, SubdomainID: 2, Variant: model.VariantMain, Name: "xn--e1afmkfd.xn--p1ai"}
	dead := Dead{
		TaskID: "rt4", Stream: StreamHTTP, EntryID: "1-1", Attempts: 2,
		ZoneID: 7, DomainID: 1000000, Name: "xn--e1afmkfd.xn--p1ai",
	}

	cases := []struct {
		stream string
		msg    Message
		decode func(Delivery) (any, error)
	}{
		{StreamDNS, batch, func(d Delivery) (any, error) { return Decode[DNSBatch](d) }},
		{StreamHTTP, task, func(d Delivery) (any, error) { return Decode[HTTPTask](d) }},
		{StreamRender, result, func(d Delivery) (any, error) { return Decode[Result](d) }},
		{StreamAnalyze, result, func(d Delivery) (any, error) { return Decode[Result](d) }},
		{StreamDead, dead, func(d Delivery) (any, error) { return Decode[Dead](d) }},
	}
	for _, c := range cases {
		data, err := marshalMessage(c.msg)
		if err != nil {
			t.Fatalf("%s: marshalMessage: %v", c.stream, err)
		}
		got, err := c.decode(Delivery{Stream: c.stream, ID: "1-0", Payload: data})
		if err != nil {
			t.Fatalf("%s: Decode: %v", c.stream, err)
		}
		if !reflect.DeepEqual(got, c.msg) {
			t.Errorf("%s: roundtrip %T:\n получил  %+v\n хочу     %+v", c.stream, c.msg, got, c.msg)
		}
	}
}

// TestMarshalFillsTaskID — пустой task_id заменяется новым при
// сериализации: у всех четырёх форматов и у значений, и у указателей.
// Значение мутируется в копии (вызывающий не видит), указатель — в
// объекте вызывающего; тест смотрит только JSON — идентификатор в
// записи новый в обоих случаях.
func TestMarshalFillsTaskID(t *testing.T) {
	domains := []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}
	subdomains := []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}
	msgs := []Message{
		DNSBatch{Domains: domains},
		&DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}},
		HTTPTask{ZoneID: 1, DomainID: 1, Name: "a.test", Subdomains: subdomains},
		&HTTPTask{ZoneID: 1, DomainID: 1, Name: "a.test", Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}},
		Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain},
		&Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain},
		Dead{Stream: StreamDNS, EntryID: "1-0", Attempts: 1},
		&Dead{Stream: StreamDNS, EntryID: "1-0", Attempts: 1},
	}
	for _, msg := range msgs {
		data, err := marshalMessage(msg)
		if err != nil {
			t.Fatalf("marshalMessage(%T): %v", msg, err)
		}
		var probe struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			t.Fatalf("разбор %s: %v", data, err)
		}
		if len(probe.TaskID) != 32 {
			t.Errorf("%T: task_id %q — ожидается новый идентификатор NewTaskID (32 символа; hex-алфавит — TestNewTaskID)", msg, probe.TaskID)
		}
	}
}

func TestNewTaskID(t *testing.T) {
	const hex = "0123456789abcdef"
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := NewTaskID()
		if len(id) != 32 {
			t.Fatalf("NewTaskID() = %q — ожидается 32 hex-символа", id)
		}
		if seen[id] {
			t.Fatalf("NewTaskID(): повтор %q на итерации %d", id, i)
		}
		seen[id] = true
		for _, r := range id {
			if !strings.ContainsRune(hex, r) {
				t.Fatalf("NewTaskID() = %q: неожиданный символ %q", id, r)
			}
		}
	}
}

// TestDecodeUnknownFields — новые поля будущих версий формата (например,
// счётчик попыток T-025) не ломают чтение.
func TestDecodeUnknownFields(t *testing.T) {
	data := []byte(`{"task_id":"t1","attempts":2,"domains":[{"zone_id":3,"domain_id":42,"name":"example.com"}]}`)
	got, err := Decode[DNSBatch](Delivery{Stream: StreamDNS, ID: "1-0", Payload: data})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Domains) != 1 || got.Domains[0].Name != "example.com" {
		t.Errorf("Decode с неизвестными полями: %+v", got)
	}
}

// TestDecodeBadData — кривые данные отвергаются с указанием стрима и записи;
// каждая пачка декодируется типом своего стрима.
func TestDecodeBadData(t *testing.T) {
	cases := []struct {
		name    string
		stream  string
		payload string
		wantErr string
		decode  func(Delivery) error
	}{
		{
			"битый JSON", StreamDNS, `{`, "unexpected end of JSON",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"массив вместо объекта", StreamDNS, `[]`, "cannot unmarshal array",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"null", StreamDNS, `null`, "пустая пачка",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"чужой формат полей", StreamDNS, `{"task_id":"t1","subdomains":[]}`, "пустая пачка",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"пустая пачка", StreamDNS, `{"task_id":"t1","domains":[]}`, "пустая пачка",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"domain_id 0", StreamDNS, `{"task_id":"t1","domains":[{"zone_id":1,"domain_id":0,"name":"a.test"}]}`, "domain_id",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"zone_id 0", StreamDNS, `{"task_id":"t1","domains":[{"zone_id":0,"domain_id":1,"name":"a.test"}]}`, "zone_id",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"пустое имя", StreamDNS, `{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":""}]}`, "пустое имя",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"имя длиннее 253", StreamDNS,
			`{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":"` + strings.Repeat("a", 254) + `"}]}`, "имя длиннее",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"нет субдоменов", StreamHTTP, `{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[]}`, "нет DNS-живых",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"бит субдомена вне диапазона", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":63,"label":"x","addrs":["192.0.2.1"]}]}`, "вне диапазона",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"кривой адрес", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":0,"label":"","addrs":["не IP"]}]}`, "адрес",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"нет адресов", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":0,"label":"","addrs":[]}]}`, "нет адресов",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"метка с точкой", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":0,"label":"a.b","addrs":["192.0.2.1"]}]}`, "метка",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"кривой вариант", StreamRender, `{"task_id":"t1","zone_id":1,"domain_id":1,"subdomain_id":0,"variant":7}`, "вариант",
			func(d Delivery) error { _, err := Decode[Result](d); return err },
		},
		{
			"attempts 0", StreamDead, `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":0,"domains":5}`, "attempts",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"кривой вариант", StreamDead, `{"task_id":"t1","stream":"q:render","entry_id":"1-0","attempts":1,"variant":7}`, "вариант",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"имя длиннее 253", StreamDead,
			`{"task_id":"t1","stream":"q:http","entry_id":"1-0","attempts":1,"name":"` + strings.Repeat("a", 254) + `"}`, "имя длиннее",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"отрицательный размер пачки", StreamDead, `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":1,"domains":-1}`, "пачки",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"бит субдомена вне диапазона", StreamDead,
			`{"task_id":"t1","stream":"q:render","entry_id":"1-0","attempts":1,"subdomain_id":63}`, "вне диапазона",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.decode(Delivery{Stream: c.stream, ID: "7-0", Payload: []byte(c.payload)})
			if err == nil {
				t.Fatalf("декодировано без ошибки, хочу ошибку с %q", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), "7-0") {
				t.Errorf("ошибка %q не содержит %q и идентификатор записи", err, c.wantErr)
			}
		})
	}
}

// TestDecodeWrongStream — сообщение не декодируется из чужого стрима
// (Result допустим в q:render и q:analyze, но не в q:dns; DNSBatch —
// только в q:dns); запись без поля data — ошибка.
func TestDecodeWrongStream(t *testing.T) {
	resultJSON := `{"task_id":"t1","zone_id":1,"domain_id":1,"subdomain_id":0,"variant":0}`
	if _, err := Decode[Result](Delivery{Stream: StreamDNS, ID: "1-0", Payload: []byte(resultJSON)}); err == nil {
		t.Error("Decode[Result] из q:dns: ожидалась ошибка стрима")
	}
	if _, err := Decode[DNSBatch](Delivery{Stream: StreamHTTP, ID: "1-0", Payload: []byte(`{"task_id":"t1"}`)}); err == nil {
		t.Error("Decode[DNSBatch] из q:http: ожидалась ошибка стрима")
	}
	if _, err := Decode[Result](Delivery{Stream: StreamRender, ID: "1-0", Payload: nil}); err == nil {
		t.Error("Decode без поля data: ожидалась ошибка")
	}
}

// TestDecodeEmptyPayload — запись без поля data отвергается ошибкой
// «без поля data» с именем стрима и записи (а не ошибкой разбора пустого
// JSON, которой мутант проверки длины payload подменяет её).
func TestDecodeEmptyPayload(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"nil", nil},
		{"пустой срез", []byte{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Decode[Result](Delivery{Stream: StreamRender, ID: "9-9", Payload: c.payload})
			if err == nil || !strings.Contains(err.Error(), "q:render: запись 9-9 без поля data") {
				t.Errorf("Decode(payload %q) = %v, хочу ошибку «q:render: запись 9-9 без поля data»", c.name, err)
			}
		})
	}
}

// TestValidateMessages — проверка значений перед публикацией (Publish
// вызывает validate до обращения к Redis).
func TestValidateMessages(t *testing.T) {
	cases := []struct {
		name    string
		msg     Message
		wantErr string
	}{
		{"пачка ок", DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}, ""},
		{"пачка пуста", DNSBatch{}, "пустая пачка"},
		{
			"http ок",
			HTTPTask{ZoneID: 1, DomainID: 1, Name: "a.test", Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}},
			"",
		},
		{
			"http без имени",
			HTTPTask{ZoneID: 1, DomainID: 1, Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}},
			"пустое имя",
		},
		{"результат ок", Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantHTTP}, ""},
		{"результат с кривым вариантом", Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.Variant(5)}, "вариант"},
		{"dead ок", Dead{TaskID: "t", Stream: StreamAnalyze, EntryID: "1-0", Attempts: 1}, ""},
		{"dead без task_id", Dead{Stream: StreamAnalyze, EntryID: "1-0", Attempts: 1}, "task_id"},
		{"dead из q:dead", Dead{TaskID: "t", Stream: StreamDead, EntryID: "1-0", Attempts: 1}, "стрим-источник"},
		{
			"dead с битым payload старой записи",
			Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, Payload: json.RawMessage(`{`)},
			"payload",
		},
		{
			"dead кривой вариант",
			Dead{TaskID: "t", Stream: StreamRender, EntryID: "1-0", Attempts: 1, Variant: model.Variant(5)},
			"вариант",
		},
		// Границы идентификаторов «≤ 0»: ноль недопустим так же, как
		// отрицательные (мутанты «< 0» и «≤ -1» пропускали бы его).
		// Каждый случай портит ровно одно поле — остальное валидно,
		// чтобы ошибка называла именно его.
		{
			"http zone_id 0",
			HTTPTask{ZoneID: 0, DomainID: 1, Name: "a.test", Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}},
			"zone_id 0 ≤ 0",
		},
		{
			"http domain_id 0",
			HTTPTask{ZoneID: 1, DomainID: 0, Name: "a.test", Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}},
			"domain_id 0 ≤ 0",
		},
		{"результат zone_id 0", Result{ZoneID: 0, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain}, "zone_id 0 ≤ 0"},
		{"результат domain_id 0", Result{ZoneID: 1, DomainID: 0, SubdomainID: 0, Variant: model.VariantMain}, "domain_id 0 ≤ 0"},
		// Бит 63 маски знаковый — единственная невалидность сообщения.
		{"результат бит 63", Result{ZoneID: 1, DomainID: 1, SubdomainID: 63, Variant: model.VariantMain}, "вне диапазона"},
		{
			"результат имя длиннее 253",
			Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain, Name: strings.Repeat("a", 254)},
			"имя длиннее 253",
		},
		{"dead пустой entry_id", Dead{TaskID: "t", Stream: StreamDNS, EntryID: "", Attempts: 1}, "пустой entry_id"},
		// Идентификаторы предмета q:dead опциональны, но заданный
		// обязан быть неотрицательным: -1 — ошибка (0 — допустим,
		// «не задан», см. «dead ок» выше).
		{"dead zone_id -1", Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, ZoneID: -1}, "отрицательны"},
		{"dead domain_id -1", Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, DomainID: -1}, "отрицательны"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.msg.validate()
			if c.wantErr == "" {
				if err != nil {
					t.Errorf("validate(%T): неожиданная ошибка %v", c.msg, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("validate(%T) = %v, хочу ошибку с %q", c.msg, err, c.wantErr)
			}
		})
	}
}

// TestValidateDNSBatchItemNumber — номер домена в ошибке пачки: счёт
// с единицы (i+1); невалидный ВТОРОЙ элемент даёт «№2» — мутанты
// i-1/i+0/i+2 напечатали бы №0/№1/№3.
func TestValidateDNSBatchItemNumber(t *testing.T) {
	cases := []struct {
		name    string
		second  DNSDomain
		wantErr string
	}{
		{"zone_id", DNSDomain{ZoneID: 0, DomainID: 1, Name: "b.test"}, "домен №2: zone_id 0 ≤ 0"},
		{"domain_id", DNSDomain{ZoneID: 1, DomainID: 0, Name: "b.test"}, "домен №2: domain_id 0 ≤ 0"},
		{"имя", DNSDomain{ZoneID: 1, DomainID: 1, Name: ""}, "домен №2: пустое имя"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			first := DNSDomain{ZoneID: 1, DomainID: 1, Name: "a.test"}
			err := DNSBatch{Domains: []DNSDomain{first, c.second}}.validate()
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("validate = %v, хочу ошибку с %q", err, c.wantErr)
			}
		})
	}
}

// TestValidateHTTPTaskSubdomainItemNumber — номер субдомена в ошибке
// задачи q:http (счёт с единицы): невалидный ВТОРОЙ субдомен даёт «№2»
// по каждой проверке цикла; тексты содержат и лимиты-литералы
// (63 октета метки, 253 октета CNAME).
func TestValidateHTTPTaskSubdomainItemNumber(t *testing.T) {
	cases := []struct {
		name     string
		second   HTTPSubdomain
		wantErrs []string
	}{
		{
			"бит субдомена вне диапазона",
			HTTPSubdomain{SubdomainID: 63, Label: "x", Addrs: []string{"192.0.2.1"}},
			[]string{"субдомен №2"},
		},
		{
			"метка длиннее 63",
			HTTPSubdomain{SubdomainID: 1, Label: strings.Repeat("a", 64), Addrs: []string{"192.0.2.1"}},
			[]string{"субдомен №2: метка", "до 63 октетов"},
		},
		{
			"нет адресов",
			HTTPSubdomain{SubdomainID: 1, Label: "x"},
			[]string{"субдомен №2: нет адресов"},
		},
		{
			"кривой адрес",
			HTTPSubdomain{SubdomainID: 1, Label: "x", Addrs: []string{"не IP"}},
			[]string{"субдомен №2: адрес"},
		},
		{
			"CNAME длиннее 253",
			HTTPSubdomain{SubdomainID: 1, Label: "x", Addrs: []string{"192.0.2.1"}, CNAME: strings.Repeat("c", 254)},
			[]string{"субдомен №2: CNAME длиннее 253"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			first := HTTPSubdomain{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}
			task := HTTPTask{
				ZoneID: 1, DomainID: 1, Name: "a.test",
				Subdomains: []HTTPSubdomain{first, c.second},
			}
			err := task.validate()
			if err == nil {
				t.Fatalf("validate: декодировано без ошибки, хочу ошибку с %v", c.wantErrs)
			}
			for _, want := range c.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не содержит %q", err, want)
				}
			}
		})
	}
}

// TestValidateExactLimits — граничные длины ТЗ 5.2 (п. 4): лимиты
// включающие — метка ровно 63 октета и имена/CNAME ровно 253 октета
// валидны во всех форматах, где проверяются. Запредельные длины —
// в TestValidateMessages и TestValidateHTTPTaskSubdomainItemNumber.
func TestValidateExactLimits(t *testing.T) {
	name253 := strings.Repeat("a", 253)
	if err := (DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: name253}}}).validate(); err != nil {
		t.Errorf("имя 253 октета (q:dns): %v", err)
	}
	task := HTTPTask{
		ZoneID: 1, DomainID: 1, Name: name253,
		Subdomains: []HTTPSubdomain{{
			SubdomainID: 1,
			Label:       strings.Repeat("l", 63),
			Addrs:       []string{"192.0.2.1"},
			CNAME:       strings.Repeat("c", 253),
		}},
	}
	if err := task.validate(); err != nil {
		t.Errorf("метка 63 и CNAME 253 октета (q:http): %v", err)
	}
	if err := (Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain, Name: name253}).validate(); err != nil {
		t.Errorf("имя 253 октета (результат): %v", err)
	}
	dead := Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, Name: name253}
	if err := dead.validate(); err != nil {
		t.Errorf("имя 253 октета (q:dead): %v", err)
	}
}

// TestDeadSubjectOf (T-105) — выжимка идентификаторов предмета задачи
// из тела исходного сообщения: пачка q:dns — размер (список не
// переносится), штучные задачи — домен, q:render/q:analyze — ключ
// строки subdomain_results; неразбирающееся тело оставляет поля
// нулевыми — перенос в q:dead не падает на чужом формате.
func TestDeadSubjectOf(t *testing.T) {
	cases := []struct {
		name    string
		stream  string
		payload string
		want    Dead
	}{
		{
			"пачка q:dns",
			StreamDNS,
			`{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":"a.test"},{"zone_id":2,"domain_id":2,"name":"b.test"},{"zone_id":1,"domain_id":3,"name":"c.test"}]}`,
			Dead{Domains: 3},
		},
		{"q:http", StreamHTTP,
			`{"task_id":"t2","zone_id":3,"domain_id":42,"name":"example.com","subdomains":[]}`,
			Dead{ZoneID: 3, DomainID: 42, Name: "example.com"}},
		{"q:render основной вариант", StreamRender,
			`{"task_id":"t3","zone_id":3,"domain_id":42,"subdomain_id":0,"variant":0,"name":"example.com"}`,
			Dead{ZoneID: 3, DomainID: 42, Name: "example.com"}},
		{"q:analyze http-вариант", StreamAnalyze,
			`{"task_id":"t4","zone_id":3,"domain_id":42,"subdomain_id":1,"variant":1,"name":"www.example.com"}`,
			Dead{ZoneID: 3, DomainID: 42, SubdomainID: 1, Variant: model.VariantHTTP, Name: "www.example.com"}},
		{"q:dns без пачки", StreamDNS, `{"task_id":"t5"}`, Dead{}},
		{"чужой формат", StreamHTTP, `{"no":"format"}`, Dead{}},
		{"мусор", StreamDNS, `не JSON`, Dead{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var d Dead
			deadSubjectOf(&d, c.stream, []byte(c.payload))
			if !reflect.DeepEqual(d, c.want) {
				t.Errorf("deadSubjectOf:\n получил  %+v\n хочу     %+v", d, c.want)
			}
		})
	}
}

// TestDecodeDeadLegacyPayload — записи q:dead до T-105 несли payload
// исходного сообщения: поле сохранено (omitempty) для чтения истории —
// Decode старых записей работает, новые payload не публикуют (bury,
// TestReliabClaimDeadByteGuard).
func TestDecodeDeadLegacyPayload(t *testing.T) {
	legacy := `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":4,` +
		`"reason":"исчерпан лимит попыток 3: доставок без подтверждения — 4",` +
		`"payload":{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":"a.test"}]}}`
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: "2-0", Payload: []byte(legacy)})
	if err != nil {
		t.Fatalf("старая запись q:dead не декодируется: %v", err)
	}
	if dead.TaskID != "t1" || dead.Stream != StreamDNS || dead.EntryID != "1-0" || dead.Attempts != 4 {
		t.Errorf("поля старой записи: %+v", dead)
	}
	if len(dead.Payload) == 0 || !json.Valid(dead.Payload) {
		t.Errorf("payload старой записи не прочитан: %q", dead.Payload)
	}
}

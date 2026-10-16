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
			"q:dead",
			Dead{
				TaskID:   "t1",
				Stream:   StreamDNS,
				EntryID:  "1718-0",
				Attempts: 3,
				Reason:   "dns-worker: write result: timeout",
				Payload:  json.RawMessage(`{"task_id":"t1","domains":[]}`),
			},
			`{"task_id":"t1","stream":"q:dns","entry_id":"1718-0","attempts":3,` +
				`"reason":"dns-worker: write result: timeout","payload":{"task_id":"t1","domains":[]}}`,
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
		Payload: json.RawMessage(`{"task_id":"rt4"}`),
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
// сериализации (значение и у значений, и у указателей).
func TestMarshalFillsTaskID(t *testing.T) {
	valid := DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}
	for _, msg := range []Message{valid, &valid} {
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
			t.Errorf("%T: task_id %q — ожидается 32 hex-символа (NewTaskID)", msg, probe.TaskID)
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
			"attempts 0", StreamDead, `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":0,"payload":{}}`, "attempts",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"пустой payload", StreamDead, `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":1}`, "payload",
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
		{"dead ок", Dead{TaskID: "t", Stream: StreamAnalyze, EntryID: "1-0", Attempts: 1, Payload: json.RawMessage(`{}`)}, ""},
		{"dead без task_id", Dead{Stream: StreamAnalyze, EntryID: "1-0", Attempts: 1, Payload: json.RawMessage(`{}`)}, "task_id"},
		{"dead из q:dead", Dead{TaskID: "t", Stream: StreamDead, EntryID: "1-0", Attempts: 1, Payload: json.RawMessage(`{}`)}, "стрим-источник"},
		{
			"dead с битым payload",
			Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, Payload: json.RawMessage(`{`)},
			"payload",
		},
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

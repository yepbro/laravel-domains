// Stream message format tests (T-024, spec 4.2): the wire format is
// fixed by exact JSON strings, the roundtrip — through Decode, malformed
// data — by decoding/validation errors.

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
		{Result{}, []string{StreamRender, StreamRenderFallback, StreamAnalyze}},
		{Dead{}, []string{StreamDead}},
	}
	for _, c := range cases {
		if got, want := c.msg.Streams(), c.want; !reflect.DeepEqual(got, want) {
			t.Errorf("%T.Streams() = %v, want %v", c.msg, got, want)
		}
	}
}

// TestMessageWireFormat fixes the exact JSON format of the messages of
// all streams: the field names and order are pinned, evolution — only by
// adding fields (readers ignore unknown fields).
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
			"result without a name",
			Result{TaskID: "t4", ZoneID: 3, DomainID: 42, SubdomainID: 0, Variant: model.VariantMain},
			`{"task_id":"t4","zone_id":3,"domain_id":42,"subdomain_id":0,"variant":0}`,
		},
		{
			"a q:dead batch entry",
			Dead{
				TaskID:   "t1",
				Stream:   StreamDNS,
				EntryID:  "1718-0",
				Attempts: 3,
				Reason:   "attempt limit 3 exhausted: 4 deliveries without ack",
				Domains:  5000,
			},
			`{"task_id":"t1","stream":"q:dns","entry_id":"1718-0","attempts":3,` +
				`"reason":"attempt limit 3 exhausted: 4 deliveries without ack","domains":5000}`,
		},
		{
			"a q:dead unit-task entry",
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
				t.Errorf("the format of %T:\n got  %s\n want %s", c.msg, got, c.want)
			}
		})
	}
}

// TestMessageRoundtrip — marshal → Decode for each stream of the message
// (Result is read both from q:render and from q:analyze).
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
		{StreamRenderFallback, result, func(d Delivery) (any, error) { return Decode[Result](d) }},
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
			t.Errorf("%s: roundtrip %T:\n got  %+v\n want %+v", c.stream, c.msg, got, c.msg)
		}
	}
}

// TestMarshalFillsTaskID — an empty task_id is replaced with a new one
// at serialization: in all four formats, for both values and pointers.
// The value is mutated in a copy (the caller does not see it), the
// pointer — in the caller's object; the test looks only at the JSON —
// the identifier in the entry is new in both cases.
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
			t.Fatalf("parsing %s: %v", data, err)
		}
		if len(probe.TaskID) != 32 {
			t.Errorf("%T: task_id %q — a fresh NewTaskID identifier expected (32 chars; the hex alphabet — TestNewTaskID)", msg, probe.TaskID)
		}
	}
}

func TestNewTaskID(t *testing.T) {
	const hex = "0123456789abcdef"
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := NewTaskID()
		if len(id) != 32 {
			t.Fatalf("NewTaskID() = %q — 32 hex chars expected", id)
		}
		if seen[id] {
			t.Fatalf("NewTaskID(): a repeat %q at iteration %d", id, i)
		}
		seen[id] = true
		for _, r := range id {
			if !strings.ContainsRune(hex, r) {
				t.Fatalf("NewTaskID() = %q: an unexpected character %q", id, r)
			}
		}
	}
}

// TestDecodeUnknownFields — new fields of future format versions (for
// example, the T-025 attempt counter) do not break reading.
func TestDecodeUnknownFields(t *testing.T) {
	data := []byte(`{"task_id":"t1","attempts":2,"domains":[{"zone_id":3,"domain_id":42,"name":"example.com"}]}`)
	got, err := Decode[DNSBatch](Delivery{Stream: StreamDNS, ID: "1-0", Payload: data})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Domains) != 1 || got.Domains[0].Name != "example.com" {
		t.Errorf("Decode with unknown fields: %+v", got)
	}
}

// TestDecodeBadData — malformed data is rejected with the stream and
// entry named; each batch is decoded by its stream's type.
func TestDecodeBadData(t *testing.T) {
	cases := []struct {
		name    string
		stream  string
		payload string
		wantErr string
		decode  func(Delivery) error
	}{
		{
			"broken JSON", StreamDNS, `{`, "unexpected end of JSON",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"an array instead of an object", StreamDNS, `[]`, "cannot unmarshal array",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"null", StreamDNS, `null`, "empty domain batch",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"a foreign field format", StreamDNS, `{"task_id":"t1","subdomains":[]}`, "empty domain batch",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"empty domain batch", StreamDNS, `{"task_id":"t1","domains":[]}`, "empty domain batch",
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
			"empty domain name", StreamDNS, `{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":""}]}`, "empty domain name",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"name longer than 253", StreamDNS,
			`{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":"` + strings.Repeat("a", 254) + `"}]}`, "name longer than",
			func(d Delivery) error { _, err := Decode[DNSBatch](d); return err },
		},
		{
			"no subdomains", StreamHTTP, `{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[]}`, "no DNS-live",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"subdomain bit out of range", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":63,"label":"x","addrs":["192.0.2.1"]}]}`, "out of range",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"a bad address", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":0,"label":"","addrs":["not-an-ip"]}]}`, "address",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"no addresses", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":0,"label":"","addrs":[]}]}`, "no addresses",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"a label with a dot", StreamHTTP,
			`{"task_id":"t1","zone_id":1,"domain_id":1,"name":"a.test","subdomains":[{"subdomain_id":0,"label":"a.b","addrs":["192.0.2.1"]}]}`, "label",
			func(d Delivery) error { _, err := Decode[HTTPTask](d); return err },
		},
		{
			"a bad variant", StreamRender, `{"task_id":"t1","zone_id":1,"domain_id":1,"subdomain_id":0,"variant":7}`, "variant",
			func(d Delivery) error { _, err := Decode[Result](d); return err },
		},
		{
			"attempts 0", StreamDead, `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":0,"domains":5}`, "attempts",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"a bad variant", StreamDead, `{"task_id":"t1","stream":"q:render","entry_id":"1-0","attempts":1,"variant":7}`, "variant",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"name longer than 253", StreamDead,
			`{"task_id":"t1","stream":"q:http","entry_id":"1-0","attempts":1,"name":"` + strings.Repeat("a", 254) + `"}`, "name longer than",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"a negative batch size", StreamDead, `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":1,"domains":-1}`, "batch size",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
		{
			"subdomain bit out of range", StreamDead,
			`{"task_id":"t1","stream":"q:render","entry_id":"1-0","attempts":1,"subdomain_id":63}`, "out of range",
			func(d Delivery) error { _, err := Decode[Dead](d); return err },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.decode(Delivery{Stream: c.stream, ID: "7-0", Payload: []byte(c.payload)})
			if err == nil {
				t.Fatalf("decoded without an error, want an error with %q", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), "7-0") {
				t.Errorf("the error %q does not contain %q and the entry identifier", err, c.wantErr)
			}
		})
	}
}

// TestDecodeWrongStream — a message is not decoded from a foreign
// stream (Result is allowed in q:render and q:analyze, but not in q:dns;
// DNSBatch — only in q:dns); an entry without the data field — an error.
func TestDecodeWrongStream(t *testing.T) {
	resultJSON := `{"task_id":"t1","zone_id":1,"domain_id":1,"subdomain_id":0,"variant":0}`
	if _, err := Decode[Result](Delivery{Stream: StreamDNS, ID: "1-0", Payload: []byte(resultJSON)}); err == nil {
		t.Error("Decode[Result] from q:dns: a stream error expected")
	}
	if _, err := Decode[DNSBatch](Delivery{Stream: StreamHTTP, ID: "1-0", Payload: []byte(`{"task_id":"t1"}`)}); err == nil {
		t.Error("Decode[DNSBatch] from q:http: a stream error expected")
	}
	if _, err := Decode[Result](Delivery{Stream: StreamRender, ID: "1-0", Payload: nil}); err == nil {
		t.Error("Decode without a data field: an error expected")
	}
}

// TestDecodeEmptyPayload — an entry without the data field is rejected
// with the "no data field" error naming the stream and entry (not by the
// empty-JSON parse error that the payload length check mutant would
// substitute for it).
func TestDecodeEmptyPayload(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"nil", nil},
		{"an empty slice", []byte{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Decode[Result](Delivery{Stream: StreamRender, ID: "9-9", Payload: c.payload})
			if err == nil || !strings.Contains(err.Error(), "q:render: entry 9-9 has no data field") {
				t.Errorf("Decode(payload %q) = %v, want the error «q:render: entry 9-9 has no data field»", c.name, err)
			}
		})
	}
}

// TestValidateMessages — the value check before publication (Publish
// calls validate before touching Redis).
func TestValidateMessages(t *testing.T) {
	cases := []struct {
		name    string
		msg     Message
		wantErr string
	}{
		{"batch ok", DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}, ""},
		{"batch empty", DNSBatch{}, "empty domain batch"},
		{
			"http ok",
			HTTPTask{ZoneID: 1, DomainID: 1, Name: "a.test", Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}},
			"",
		},
		{
			"http without a name",
			HTTPTask{ZoneID: 1, DomainID: 1, Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}},
			"empty domain name",
		},
		{"result ok", Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantHTTP}, ""},
		{"result with a bad variant", Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.Variant(5)}, "variant"},
		{"dead ok", Dead{TaskID: "t", Stream: StreamAnalyze, EntryID: "1-0", Attempts: 1}, ""},
		{"dead from q:render-fallback", Dead{TaskID: "t", Stream: StreamRenderFallback, EntryID: "1-0", Attempts: 1}, ""},
		{"dead without task_id", Dead{Stream: StreamAnalyze, EntryID: "1-0", Attempts: 1}, "task_id"},
		{"dead from q:dead", Dead{TaskID: "t", Stream: StreamDead, EntryID: "1-0", Attempts: 1}, "source stream"},
		{
			"dead with a broken legacy-entry payload",
			Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, Payload: json.RawMessage(`{`)},
			"payload",
		},
		{
			"dead with a bad variant",
			Dead{TaskID: "t", Stream: StreamRender, EntryID: "1-0", Attempts: 1, Variant: model.Variant(5)},
			"variant",
		},
		// The identifier boundaries "≤ 0": zero is invalid just like the
		// negatives (the mutants "< 0" and "≤ -1" would let it through).
		// Each case corrupts exactly one field — the rest is valid, so
		// that the error names exactly that field.
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
		{"result zone_id 0", Result{ZoneID: 0, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain}, "zone_id 0 ≤ 0"},
		{"result domain_id 0", Result{ZoneID: 1, DomainID: 0, SubdomainID: 0, Variant: model.VariantMain}, "domain_id 0 ≤ 0"},
		// Bit 63 of the mask is signed — the only invalidity of the
		// message.
		{"result bit 63", Result{ZoneID: 1, DomainID: 1, SubdomainID: 63, Variant: model.VariantMain}, "out of range"},
		{
			"result name longer than 253",
			Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain, Name: strings.Repeat("a", 254)},
			"name longer than 253",
		},
		{"dead empty entry_id", Dead{TaskID: "t", Stream: StreamDNS, EntryID: "", Attempts: 1}, "empty entry_id"},
		// The q:dead subject identifiers are optional, but a set one
		// must be non-negative: -1 is an error (0 is allowed, "not set",
		// see "dead ok" above).
		{"dead zone_id -1", Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, ZoneID: -1}, "are negative"},
		{"dead domain_id -1", Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, DomainID: -1}, "are negative"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.msg.validate()
			if c.wantErr == "" {
				if err != nil {
					t.Errorf("validate(%T): an unexpected error %v", c.msg, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("validate(%T) = %v, want an error with %q", c.msg, err, c.wantErr)
			}
		})
	}
}

// TestValidateDNSBatchItemNumber — the domain number in the batch
// error: counting from one (i+1); an invalid SECOND element gives "#2" —
// the mutants i-1/i+0/i+2 would print #0/#1/#3.
func TestValidateDNSBatchItemNumber(t *testing.T) {
	cases := []struct {
		name    string
		second  DNSDomain
		wantErr string
	}{
		{"zone_id", DNSDomain{ZoneID: 0, DomainID: 1, Name: "b.test"}, "domain #2: zone_id 0 ≤ 0"},
		{"domain_id", DNSDomain{ZoneID: 1, DomainID: 0, Name: "b.test"}, "domain #2: domain_id 0 ≤ 0"},
		{"name", DNSDomain{ZoneID: 1, DomainID: 1, Name: ""}, "domain #2: empty domain name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			first := DNSDomain{ZoneID: 1, DomainID: 1, Name: "a.test"}
			err := DNSBatch{Domains: []DNSDomain{first, c.second}}.validate()
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("validate = %v, want an error with %q", err, c.wantErr)
			}
		})
	}
}

// TestValidateHTTPTaskSubdomainItemNumber — the subdomain number in the
// q:http task error (counting from one): an invalid SECOND subdomain
// gives "#2" in every loop check; the texts contain the literal limits
// too (63 octets of the label, 253 octets of the CNAME).
func TestValidateHTTPTaskSubdomainItemNumber(t *testing.T) {
	cases := []struct {
		name     string
		second   HTTPSubdomain
		wantErrs []string
	}{
		{
			"subdomain bit out of range",
			HTTPSubdomain{SubdomainID: 63, Label: "x", Addrs: []string{"192.0.2.1"}},
			[]string{"subdomain #2"},
		},
		{
			"label longer than 63",
			HTTPSubdomain{SubdomainID: 1, Label: strings.Repeat("a", 64), Addrs: []string{"192.0.2.1"}},
			[]string{"subdomain #2: label", "up to 63 octets"},
		},
		{
			"no addresses",
			HTTPSubdomain{SubdomainID: 1, Label: "x"},
			[]string{"subdomain #2: no addresses"},
		},
		{
			"a bad address",
			HTTPSubdomain{SubdomainID: 1, Label: "x", Addrs: []string{"not-an-ip"}},
			[]string{"subdomain #2: address"},
		},
		{
			"CNAME longer than 253",
			HTTPSubdomain{SubdomainID: 1, Label: "x", Addrs: []string{"192.0.2.1"}, CNAME: strings.Repeat("c", 254)},
			[]string{"subdomain #2: CNAME longer than 253"},
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
				t.Fatalf("validate: decoded without an error, want an error with %v", c.wantErrs)
			}
			for _, want := range c.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the error %q does not contain %q", err, want)
				}
			}
		})
	}
}

// TestValidateExactLimits — the boundary lengths of spec 5.2 (p. 4):
// the limits are inclusive — a label of exactly 63 octets and names/
// CNAME of exactly 253 octets are valid in every format where they are
// checked. The over-limit lengths — in TestValidateMessages and
// TestValidateHTTPTaskSubdomainItemNumber.
func TestValidateExactLimits(t *testing.T) {
	name253 := strings.Repeat("a", 253)
	if err := (DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: name253}}}).validate(); err != nil {
		t.Errorf("a 253-octet name (q:dns): %v", err)
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
		t.Errorf("a 63-octet label and a 253-octet CNAME (q:http): %v", err)
	}
	if err := (Result{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: model.VariantMain, Name: name253}).validate(); err != nil {
		t.Errorf("a 253-octet name (result): %v", err)
	}
	dead := Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1, Name: name253}
	if err := dead.validate(); err != nil {
		t.Errorf("a 253-octet name (q:dead): %v", err)
	}
}

// TestDeadSubjectOf (T-105) — the distillation of the task subject
// identifiers from the original message body: a q:dns batch — the size
// (the list is not carried), one-off tasks — the domain,
// q:render/q:analyze — the subdomain_results row key; a body that does
// not parse leaves the fields zero — the move to q:dead does not fail
// on a foreign format.
func TestDeadSubjectOf(t *testing.T) {
	cases := []struct {
		name    string
		stream  string
		payload string
		want    Dead
	}{
		{
			"a q:dns batch",
			StreamDNS,
			`{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":"a.test"},{"zone_id":2,"domain_id":2,"name":"b.test"},{"zone_id":1,"domain_id":3,"name":"c.test"}]}`,
			Dead{Domains: 3},
		},
		{"q:http", StreamHTTP,
			`{"task_id":"t2","zone_id":3,"domain_id":42,"name":"example.com","subdomains":[]}`,
			Dead{ZoneID: 3, DomainID: 42, Name: "example.com"}},
		{"q:render main variant", StreamRender,
			`{"task_id":"t3","zone_id":3,"domain_id":42,"subdomain_id":0,"variant":0,"name":"example.com"}`,
			Dead{ZoneID: 3, DomainID: 42, Name: "example.com"}},
		{"q:render-fallback http variant", StreamRenderFallback,
			`{"task_id":"t6","zone_id":3,"domain_id":42,"subdomain_id":2,"variant":1,"name":"www.example.com"}`,
			Dead{ZoneID: 3, DomainID: 42, SubdomainID: 2, Variant: model.VariantHTTP, Name: "www.example.com"}},
		{"q:analyze http variant", StreamAnalyze,
			`{"task_id":"t4","zone_id":3,"domain_id":42,"subdomain_id":1,"variant":1,"name":"www.example.com"}`,
			Dead{ZoneID: 3, DomainID: 42, SubdomainID: 1, Variant: model.VariantHTTP, Name: "www.example.com"}},
		{"q:dns without a batch", StreamDNS, `{"task_id":"t5"}`, Dead{}},
		{"a foreign format", StreamHTTP, `{"no":"format"}`, Dead{}},
		{"junk", StreamDNS, `not JSON`, Dead{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var d Dead
			deadSubjectOf(&d, c.stream, []byte(c.payload))
			if !reflect.DeepEqual(d, c.want) {
				t.Errorf("deadSubjectOf:\n got  %+v\n want %+v", d, c.want)
			}
		})
	}
}

// TestDecodeDeadLegacyPayload — q:dead entries before T-105 carried the
// payload of the original message: the field is kept (omitempty) for
// history reading — Decode of old entries works, new ones do not
// publish payload (bury, TestReliabClaimDeadByteGuard).
func TestDecodeDeadLegacyPayload(t *testing.T) {
	legacy := `{"task_id":"t1","stream":"q:dns","entry_id":"1-0","attempts":4,` +
		`"reason":"attempt limit 3 exhausted: 4 deliveries without ack",` +
		`"payload":{"task_id":"t1","domains":[{"zone_id":1,"domain_id":1,"name":"a.test"}]}}`
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: "2-0", Payload: []byte(legacy)})
	if err != nil {
		t.Fatalf("the legacy q:dead entry does not decode: %v", err)
	}
	if dead.TaskID != "t1" || dead.Stream != StreamDNS || dead.EntryID != "1-0" || dead.Attempts != 4 {
		t.Errorf("the legacy entry's fields: %+v", dead)
	}
	if len(dead.Payload) == 0 || !json.Valid(dead.Payload) {
		t.Errorf("the legacy entry's payload not read: %q", dead.Payload)
	}
}

// Stream message formats (spec 4.2: "the message contains only
// identifiers and the minimally necessary data (the domain name)").
//
// A stream entry stores a single field data — the JSON representation of
// the message (write/read — redisq.go). JSON, not bitwise packing: the
// messages are tiny (identifiers and the domain name, tens of bytes), the
// gain of a compact format is percent-level, while readability in
// redis-cli (XRANGE shows ready JSON) and freedom of evolution matter
// more: adding a field does not break readers — unknown JSON fields are
// ignored, so T-025 introduced the delivery attempt counter in the same
// format.
//
// The first field of every message is task_id, a unique task identifier
// (NewTaskID). It survives the stream entry: when the attempts are
// exhausted, the message is moved to q:dead with the same task_id (T-025),
// which ties together the original task, repeated deliveries and manual
// triage.

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

// Name maxima per spec 5.2 (p. 4): label ≤ 63, full name ≤ 253 octets in
// ASCII form. The queue does not normalize names again (that is done at
// the input — internal/idn), it only rejects clearly impossible ones.
const (
	maxLabelLen = 63
	maxNameLen  = 253
)

// Message — a message of one of the queue streams: DNSBatch (q:dns),
// HTTPTask (q:http), Result (q:render, q:render-fallback, q:analyze),
// Dead (q:dead). The interface is sealed by an unexported validate method:
// the stream formats are introduced only by the queue package, third-party
// types must not be published.
//
// The names are the ASCII form (punycode, spec 5.2): in the queue, as in
// the whole system, only ASCII names circulate.
type Message interface {
	// Streams returns the streams the message may be published to
	// (spec 4.2). Result has three of them: q:render, q:render-fallback
	// (the engine fallback queue, spec 4.3) and q:analyze share the same
	// "one result" format.
	Streams() []string

	// validate checks the minimal admissibility of values before
	// publication and after decoding.
	validate() error
}

// DNSDomain — a domain of the q:dns batch: the identifiers of the domains
// row and the name (spec 4.2: "a batch of domains (id + name)"). dns-worker
// makes queries by name, writes results by identifiers; zone_id is included
// as the row identifier — the partitioned result tables need it and it
// spares the worker a backward read of domains.
type DNSDomain struct {
	ZoneID   int16  `json:"zone_id"`
	DomainID int64  `json:"domain_id"`
	Name     string `json:"name"` // full name, punycode
}

// DNSBatch — a q:dns message: a batch of domains for the DNS check
// (spec 4.2, §5.3 — the scheduler publishes in batches; the batch size is
// scheduler.batch_size).
type DNSBatch struct {
	TaskID  string      `json:"task_id"`
	Domains []DNSDomain `json:"domains"`
}

// Streams — the stream of DNS task batches.
func (DNSBatch) Streams() []string { return []string{StreamDNS} }

func (b DNSBatch) validate() error {
	if len(b.Domains) == 0 {
		return fmt.Errorf("queue: q:dns: empty domain batch")
	}
	for i, d := range b.Domains {
		if d.ZoneID <= 0 {
			return fmt.Errorf("queue: q:dns: domain #%d: zone_id %d ≤ 0", i+1, d.ZoneID)
		}
		if d.DomainID <= 0 {
			return fmt.Errorf("queue: q:dns: domain #%d: domain_id %d ≤ 0", i+1, d.DomainID)
		}
		if err := checkName(d.Name); err != nil {
			return fmt.Errorf("queue: q:dns: domain #%d: %w", i+1, err)
		}
	}
	return nil
}

// HTTPSubdomain — a DNS-live subdomain of a q:http task: the bit number of
// dns_mask (subdomains.id), the label ("" — the domain apex) and the
// addresses with CNAME from the DNS stage.
type HTTPSubdomain struct {
	SubdomainID int16    `json:"subdomain_id"`
	Label       string   `json:"label"`           // "" — the apex
	Addrs       []string `json:"addrs"`           // A/AAAA, already resolved
	CNAME       string   `json:"cname,omitempty"` // when the subdomain is a CNAME
}

// HTTPTask — a q:http message: one domain with the list of its DNS-live
// subdomains (spec 4.2). Addresses and CNAME are included per spec 5.4
// (p. 4: "the addresses and CNAME are passed in the q:http task") —
// http-worker connects to pre-resolved IPs (spec 5.5, p. 2) and makes no
// repeated DNS queries for the task's subdomains.
type HTTPTask struct {
	TaskID     string          `json:"task_id"`
	ZoneID     int16           `json:"zone_id"`
	DomainID   int64           `json:"domain_id"`
	Name       string          `json:"name"` // full domain name, punycode
	Subdomains []HTTPSubdomain `json:"subdomains"`
}

// Streams — the stream of HTTP tasks.
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
		return fmt.Errorf("queue: q:http: no DNS-live subdomains (the task is not published, spec 5.4 p. 5)")
	}
	for i, s := range t.Subdomains {
		if err := model.ValidateSubdomainID(int(s.SubdomainID)); err != nil {
			return fmt.Errorf("queue: q:http: subdomain #%d: %w", i+1, err)
		}
		if len(s.Label) > maxLabelLen || strings.ContainsAny(s.Label, ". ") {
			return fmt.Errorf("queue: q:http: subdomain #%d: label %q is a single DNS label of up to %d octets", i+1, s.Label, maxLabelLen)
		}
		if len(s.Addrs) == 0 {
			return fmt.Errorf("queue: q:http: subdomain #%d: no addresses", i+1)
		}
		for _, a := range s.Addrs {
			if _, err := netip.ParseAddr(a); err != nil {
				return fmt.Errorf("queue: q:http: subdomain #%d: address %q: %w", i+1, a, err)
			}
		}
		if len(s.CNAME) > maxNameLen {
			return fmt.Errorf("queue: q:http: subdomain #%d: CNAME longer than %d octets", i+1, maxNameLen)
		}
	}
	return nil
}

// Result — a message of q:render, q:render-fallback and q:analyze (T-237,
// spec 4.3 p.1: the fallback queue uses the same contract — the message is
// self-sufficient, no conversion is needed): one result — the key of a
// subdomain_results row (domain, subdomain, variant; spec 4.2, §6.1). The
// result itself stays in the DB, the message only says which record to
// process (render takes the final URL, the analyzer — the content by key);
// the domain name is the minimal datum for logs and manual triage
// (spec 4.2).
type Result struct {
	TaskID      string        `json:"task_id"`
	ZoneID      int16         `json:"zone_id"`
	DomainID    int64         `json:"domain_id"`
	SubdomainID int16         `json:"subdomain_id"`
	Variant     model.Variant `json:"variant"`        // 0 main, 1 http (spec 6.1)
	Name        string        `json:"name,omitempty"` // full name, punycode
	// Fresh — the row was written by a new HTTP check episode (T-118):
	// publication by the HTTP/render stage (in the same transaction as the
	// episode) — true, re-analysis over the stored content (ReanalyzeLoop,
	// T-074) — false. The analyzer reads it: the laravel_history detection
	// history (appeared/lost) is written only for fresh episodes — a rule
	// change is not a fact about the site. omitempty: entries written
	// before T-118 are read as false (re-analysis); the old and new
	// formats are compatible without a stream migration.
	Fresh bool `json:"fresh,omitempty"`
}

// Streams — the result streams: one format; render (the main engine and
// the fallback worker B) and analysis publish the same message.
func (Result) Streams() []string {
	return []string{StreamRender, StreamRenderFallback, StreamAnalyze}
}

func (r Result) validate() error {
	if r.ZoneID <= 0 {
		return fmt.Errorf("queue: result: zone_id %d ≤ 0", r.ZoneID)
	}
	if r.DomainID <= 0 {
		return fmt.Errorf("queue: result: domain_id %d ≤ 0", r.DomainID)
	}
	if err := model.ValidateSubdomainID(int(r.SubdomainID)); err != nil {
		return fmt.Errorf("queue: result: %w", err)
	}
	if !r.Variant.Valid() {
		return fmt.Errorf("queue: result: variant %d (0 main, 1 http)", r.Variant)
	}
	if len(r.Name) > maxNameLen {
		return fmt.Errorf("queue: result: name longer than %d octets", maxNameLen)
	}
	return nil
}

// Dead — a q:dead message: a task that exhausted its attempt count
// (spec 4.2), for manual triage. The producer is T-025 (the attempt counter
// and the move to q:dead); the format is fixed here in advance so that the
// evolution of delivery does not change the stream contract. The entries
// carry the identifiers of the task's subject, not the original message
// (T-105): the payload of q:dns batches of the batch_size 5000 era (~250 KB
// per entry) with a cap of 10,000 entries gave a worst case of ~2.5 GB
// against the Redis budget of ≤ 0.5 GB (spec 3.2, docs/redis.md §3), while
// manual triage has enough with the identifiers (docs/runbooks.md §2:
// domain — name/domain_id, the row key — zone/domain/subdomain/variant;
// the q:dns batch domains will be re-published by the scheduler's lease
// pass). Payload is kept only for reading old entries (before T-105) —
// new ones do not publish it.
type Dead struct {
	TaskID   string `json:"task_id"`          // task_id of the original task
	Stream   string `json:"stream"`           // source stream
	EntryID  string `json:"entry_id"`         // source entry
	Attempts int    `json:"attempts"`         // number of delivery attempts
	Reason   string `json:"reason,omitempty"` // reason the attempts ran out

	// The identifiers of the task's subject (T-105) — a distillation of the
	// original message (deadSubjectOf). Optional: the move to q:dead must
	// not fail on a body that does not parse as a stream message.
	ZoneID      int16         `json:"zone_id,omitempty"`      // one-off tasks: the domain's zone
	DomainID    int64         `json:"domain_id,omitempty"`    // one-off tasks: the domain
	SubdomainID int16         `json:"subdomain_id,omitempty"` // q:render/q:analyze: the row key
	Variant     model.Variant `json:"variant,omitempty"`      // q:render/q:analyze: 0 is omitted (main)
	Name        string        `json:"name,omitempty"`         // the domain name, punycode
	Domains     int           `json:"domains,omitempty"`      // q:dns: the batch size

	// Payload — the original message of the entry, pre-T-105; it is not
	// published by new entries, it is read for triage history and ignored
	// by triage.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Streams — the dead-task stream.
func (Dead) Streams() []string { return []string{StreamDead} }

func (d Dead) validate() error {
	if d.TaskID == "" {
		return fmt.Errorf("queue: q:dead: empty task_id")
	}
	switch d.Stream {
	// The branch body is empty (valid streams have nothing to check):
	// branch/case replaces it with a noop EmptyStmt whose printout is
	// identical to the original — a mutant with an empty diff that no test
	// can kill; suppressing the generation — an alternative to the
	// checksum blacklist (docs/mutation-testing.md §4). Remove the
	// annotation if the branch ever gains a body.
	// mutator-disable-next-line branch/case
	case StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze:
	default:
		return fmt.Errorf("queue: q:dead: source stream %q is not a working task stream", d.Stream)
	}
	if d.EntryID == "" {
		return fmt.Errorf("queue: q:dead: empty entry_id")
	}
	if d.Attempts < 1 {
		return fmt.Errorf("queue: q:dead: attempts %d < 1", d.Attempts)
	}
	// The subject identifiers (T-105) are optional (deadSubjectOf is
	// best-effort); the ones that are set must be valid.
	if d.ZoneID < 0 || d.DomainID < 0 {
		return fmt.Errorf("queue: q:dead: domain identifiers %d/%d are negative", d.ZoneID, d.DomainID)
	}
	if d.Domains < 0 {
		return fmt.Errorf("queue: q:dead: batch size %d < 0", d.Domains)
	}
	if len(d.Name) > maxNameLen {
		return fmt.Errorf("queue: q:dead: name longer than %d octets", maxNameLen)
	}
	if !d.Variant.Valid() {
		return fmt.Errorf("queue: q:dead: variant %d (0 main, 1 http)", d.Variant)
	}
	if err := model.ValidateSubdomainID(int(d.SubdomainID)); err != nil {
		return fmt.Errorf("queue: q:dead: %w", err)
	}
	if len(d.Payload) > 0 && !json.Valid(d.Payload) {
		return fmt.Errorf("queue: q:dead: payload is the original message of a legacy entry (JSON), %d bytes set", len(d.Payload))
	}
	return nil
}

// deadSubjectOf distills from the body of the original message the
// identifiers of the task's subject into a Dead entry (T-105): q:dns — the
// batch size (the list of domains is not carried — up to ~250 KB per
// entry, see Dead), q:http — the domain, q:render/q:render-fallback/
// q:analyze (a Result body) — the subdomain_results row key.
// Best-effort: a body that does not parse as a stream message leaves the
// fields zero; one that parses but is foreign (invalid values) — bury
// falls back to an entry without identifiers — so the move to q:dead
// neither fails nor gets stuck on any body with a task_id.
func deadSubjectOf(d *Dead, stream string, payload []byte) {
	switch stream {
	case StreamDNS:
		// The batch elements are not read — only their number: []struct{}
		// parses the array without allocating names and identifiers.
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
	case StreamRender, StreamRenderFallback, StreamAnalyze:
		var m Result
		if json.Unmarshal(payload, &m) == nil {
			d.ZoneID, d.DomainID = m.ZoneID, m.DomainID
			d.SubdomainID, d.Variant, d.Name = m.SubdomainID, m.Variant, m.Name
		}
	}
}

// NewTaskID generates a unique task identifier: 16 random bytes
// (crypto/rand) in hex — 128 bits of randomness, collisions are practically
// excluded. The identifier lives in the message (not in the stream entry):
// moving a task to q:dead (T-025) keeps it to link with the original task
// and the logs.
func NewTaskID() string {
	var b [16]byte
	// Since Go 1.24 crypto/rand.Read returns no errors (it fills the
	// buffer completely or panics).
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// marshalMessage serializes the message into the JSON data field. An empty
// TaskID is replaced with a new one (a copy of the message — the change is
// invisible to the caller; the identifier stays inside the queue entry).
// If the identifier is needed in advance — fill the field with NewTaskID()
// before publishing.
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
		return nil, fmt.Errorf("queue: unknown message type %T (formats are defined only by the queue package)", msg)
	}
}

// Decode decodes the delivery payload into a message of type T, checking
// the delivery stream and the admissibility of the values:
//
//	batch, err := queue.Decode[queue.DNSBatch](d)
//
// Unknown JSON fields are ignored — new fields of future versions (the
// T-025 attempt counter) do not break reading.
func Decode[T Message](d Delivery) (T, error) {
	var m T
	if len(d.Payload) == 0 {
		return m, fmt.Errorf("queue: %s: entry %s has no data field", d.Stream, d.ID)
	}
	if err := json.Unmarshal(d.Payload, &m); err != nil {
		return m, fmt.Errorf("queue: %s: entry %s: %w", d.Stream, d.ID, err)
	}
	if !streamAllowed(m, d.Stream) {
		return m, fmt.Errorf("queue: %s: entry %s: message %T does not belong to this stream (allowed: %v)",
			d.Stream, d.ID, m, m.Streams())
	}
	if err := m.validate(); err != nil {
		return m, fmt.Errorf("queue: %s: entry %s: %w", d.Stream, d.ID, err)
	}
	return m, nil
}

// streamAllowed reports that the message msg may sit in the stream stream.
func streamAllowed(msg Message, stream string) bool {
	for _, s := range msg.Streams() {
		if s == stream {
			return true
		}
	}
	return false
}

// checkName checks the full domain name: non-empty and within the spec 5.2
// limit (p. 4). The name is expected to be already normalized (punycode);
// a deep syntax check is the territory of internal/idn at the system input.
func checkName(name string) error {
	if name == "" {
		return fmt.Errorf("empty domain name")
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("name longer than %d octets", maxNameLen)
	}
	return nil
}

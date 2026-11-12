// Unit tests of the scheduler batch selection (T-027, spec 5.3) without a DB (the
// internal/ingest/apply_add_test.go / zones_test.go style): exact SQL texts
// of both queries (active zones with work — domains_pending > 0, T-183;
// LATERAL queue prefixes) and their parameters (arrays of zones/keyset cursors,
// the quota — the batch remainder in top-ups, T-183), round-robin merging with
// weights, top-up of starved zones, an empty result, validation
// of parameters, query/read errors, ctx cancellation. The real behavior
// against a live DB — the scheduler integration tests.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/migrations"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- fake pgx pool -----------------------------------------------

// dispatchPool — a db.DB for DispatchBatch: remembers the SQL and arguments
// of Query, returns pre-made rows (or an error) in call order.
// Like real pgx, it refuses to work with a cancelled context.
type dispatchPool struct {
	queries []dispatchPoolQuery
	results []*dispatchRows
	errs    []error // an error per each individual Query call (nil — success): injecting a failure into the middle of the dialogue
	err     error   // the error of Query itself, if set
}

type dispatchPoolQuery struct {
	sql  string
	args []any
}

var _ db.DB = (*dispatchPool)(nil)

func (p *dispatchPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("dispatchPool: CopyFrom must not be called")
}

func (p *dispatchPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("dispatchPool: Exec must not be called")
}

func (p *dispatchPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.queries = append(p.queries, dispatchPoolQuery{sql: sql, args: args})
	if len(p.errs) > 0 {
		err := p.errs[0]
		p.errs = p.errs[1:]
		if err != nil {
			return nil, err
		}
	} else if p.err != nil {
		return nil, p.err
	}
	if len(p.results) == 0 {
		return nil, errors.New("dispatchPool: the Query result is not stubbed")
	}
	rows := p.results[0]
	p.results = p.results[1:]
	return rows, nil
}

// dispatchRows — the pgx.Rows of a response: column names and rows; Scan
// distributes the values positionally by the receiver's type (int16/int64/
// string — like pgx by the OIDs of the columns of both selection queries).
type dispatchRows struct {
	colNames []string
	rows     [][]any
	i        int // the index of the current row (0 — before the first Next)
	closed   bool
	err      error // Err() after exhaustion, if set
	scanErr  error // a Scan error, if set
}

func (r *dispatchRows) Close() { r.closed = true }

func (r *dispatchRows) Err() error { return r.err }

func (r *dispatchRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *dispatchRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(r.colNames))
	for i, c := range r.colNames {
		fds[i] = pgconn.FieldDescription{Name: c}
	}
	return fds
}

func (r *dispatchRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *dispatchRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("dispatchRows: Scan without Next")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return errors.New("dispatchRows: the sink count does not match the column count")
	}
	for i := range row {
		switch d := dest[i].(type) {
		case *int16:
			*d = row[i].(int16)
		case *int64:
			*d = row[i].(int64)
		case *string:
			*d = row[i].(string)
		default:
			return errors.New("dispatchRows: unknown sink")
		}
	}
	return nil
}

func (r *dispatchRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *dispatchRows) RawValues() [][]byte    { return nil }

// Conn and TypeMap — part of pgx.Rows not used by manual reading.
func (r *dispatchRows) Conn() *pgx.Conn      { return nil }
func (r *dispatchRows) TypeMap() *pgtype.Map { return nil }

// --- response stubs --------------------------------------------------------

// The columns of the selection queries in SELECT order.
var (
	dispatchZonesCols  = []string{"id", "name"}
	dispatchDomainCols = []string{"zone_id", "id", "label"}
)

// dispatchZonesResult — the active zones query response: rows (id, name).
func dispatchZonesResult(rows ...[]any) *dispatchRows {
	return &dispatchRows{colNames: dispatchZonesCols, rows: rows}
}

// dispatchDomainsResult — the queues query response: rows
// (zone_id, id, label) in any order — the merge sorts itself.
func dispatchDomainsResult(rows ...[]any) *dispatchRows {
	return &dispatchRows{colNames: dispatchDomainCols, rows: rows}
}

// zrow — a zones row: (id, name).
func zrow(id int16, name string) []any { return []any{id, name} }

// drow — a queue row: (zone_id, id, label).
func drow(zone int16, id int64, label string) []any { return []any{zone, id, label} }

// drowID — a drow with the label "d<id>" for multi-row batches of merge tests:
// the full domain name in expectations — "d<id>.<zone>".
func drowID(zone int16, id int64) []any { return drow(zone, id, fmt.Sprintf("d%d", id)) }

// drowIDs — the rows of one zone by an id list (ids ascending — the
// zone's queue; shuffling is checked by TestDispatchBatch separately).
func drowIDs(zone int16, ids ...int64) []any {
	rows := make([]any, len(ids))
	for i, id := range ids {
		rows[i] = drowID(zone, id)
	}
	return rows
}

// drows — a stub response of the queues query from rows assembled
// by append (the elements are []any of []any — as in drow/drowIDs).
func drows(rows []any) *dispatchRows {
	out := make([][]any, len(rows))
	for i, r := range rows {
		out[i] = r.([]any)
	}
	return dispatchDomainsResult(out...)
}

// --- selection ------------------------------------------------------------

func TestDispatchBatch(t *testing.T) {
	// The zone rows are shuffled and arrive within a zone not by id — the merge
	// must sort the zone queue and assemble the round-robin order.
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(
			drow(2, 201, "b"),
			drow(1, 102, "c"),
			drow(1, 101, "a"),
		),
	}}
	q := DispatchQuery{Limit: 3, ZoneWeights: map[int16]int{2: 3, 1: 1}}

	batch, err := DispatchBatch(context.Background(), pool, q)
	if err != nil {
		t.Fatal(err)
	}
	// Zone 2 weight = 3: its first row goes first (key 0.5/3 < 0.5/1),
	// then — interleaving by the key (rn − 0.5)/weight.
	want := []DispatchRow{
		{DomainID: 201, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 102, ZoneID: 1, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (round-robin order with weights)", batch, want)
	}

	if len(pool.queries) != 2 {
		t.Fatalf("queries = %d, want 2 (zones + queues)", len(pool.queries))
	}
	if pool.queries[0].sql != dispatchZonesSQL || len(pool.queries[0].args) != 0 {
		t.Errorf("the zones query: %q args=%v, want dispatchZonesSQL without parameters", pool.queries[0].sql, pool.queries[0].args)
	}
	if pool.queries[1].sql != dispatchZoneSQL {
		t.Errorf("the queues query: %q, want dispatchZoneSQL", pool.queries[1].sql)
	}
	// Arguments: zones, keyset cursors (0 — from the head) and the quota —
	// ceil(L × maxW / ΣW) + 1 = ceil(3 × 3 / 4) + 1 = 4.
	wantArgs := []any{[]any{int16(1), int16(2)}, []any{int64(0), int64(0)}, 4}
	if !reflect.DeepEqual(pool.queries[1].args, wantArgs) {
		t.Errorf("args = %v, want [zone_id[], after[], quota] = %v", pool.queries[1].args, wantArgs)
	}
}

// TestDispatchBatch_Refill — exhausted zones give their shares to neighbors:
// the read prefix was not enough, the "starved" zone (it returned exactly
// the quota) is re-queried with a keyset cursor from the last id.
func TestDispatchBatch_Refill(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		// Quota ceil(4 × 1 / 2) + 1 = 3: zone 1 is truncated (starved),
		// zone 2 is exhausted (less than the quota).
		dispatchDomainsResult(
			drow(1, 11, "a"), drow(1, 12, "b"), drow(1, 13, "c"),
		),
		// Top-up: only zone 1, after id 13, the quota — the batch remainder.
		dispatchDomainsResult(drow(1, 14, "d")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 12, ZoneID: 1, Name: "b.example.com"},
		{DomainID: 13, ZoneID: 1, Name: "c.example.com"},
		{DomainID: 14, ZoneID: 1, Name: "d.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v", batch, want)
	}

	if len(pool.queries) != 3 {
		t.Fatalf("queries = %d, want 3 (zones + queues + top-up)", len(pool.queries))
	}
	// Top-up: only zone 1, after id 13, the quota — the batch remainder
	// (4 − 3 = 1): a zone's contribution to the batch does not exceed the remainder (T-183).
	wantArgs := []any{[]any{int16(1)}, []any{int64(13)}, 1}
	if !reflect.DeepEqual(pool.queries[2].args, wantArgs) {
		t.Errorf("top-up args = %v, want %v", pool.queries[2].args, wantArgs)
	}
}

// TestDispatchBatch_RefillQuotaRemaining — the top-up round quota is the
// batch remainder (Limit − collected), not Limit (T-183): several starved
// zones, remaining > 1 < Limit — the argument of the top-up LATERAL query
// equals the remainder; more than the remainder is unavailable to one zone, the batch fills up.
func TestDispatchBatch_RefillQuotaRemaining(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(
			zrow(1, "example.com"), zrow(2, "example.net"), zrow(3, "example.org"),
			zrow(4, "example.dev"), zrow(5, "example.io"), zrow(6, "example.app"),
		),
		// Quota ceil(10 × 1 / 6) + 1 = 3: zones 1 and 2 are truncated (starved),
		// zones 3–6 gave everything (empty).
		drows(append(drowIDs(1, 11, 12, 13), drowIDs(2, 21, 22, 23)...)),
		// Top-up: zone 1 is exhausted (2 tail rows), zone 2
		// gives the remainder in full (4 = the round quota).
		drows(append(drowIDs(1, 14, 15), drowIDs(2, 24, 25, 26, 27)...)),
		// The batch is full (5 + 7 ≥ 10) — there is no third round.
		dispatchDomainsResult(),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	// Round-robin order of weights 1: interleaving zones 1 and 2, equal keys —
	// the smaller zone_id; the batch is truncated by Limit = 10.
	wantIDs := []int64{11, 21, 12, 22, 13, 23, 14, 24, 15, 25}
	if len(batch) != len(wantIDs) {
		t.Fatalf("batch = %d rows, want %d", len(batch), len(wantIDs))
	}
	for i, id := range wantIDs {
		if batch[i].DomainID != id {
			t.Fatalf("batch[%d] = domain %d, want %d (round-robin order)", i, batch[i].DomainID, id)
		}
	}

	if len(pool.queries) != 3 {
		t.Fatalf("queries = %d, want 3 (zones + queues + top-up)", len(pool.queries))
	}
	// The top-up quota is the batch remainder: 10 − 6 collected = 4 (not
	// Limit = 10): reading beyond the remainder is pointless — beyond it nothing
	// gets into the merge.
	wantArgs := []any{[]any{int16(1), int16(2)}, []any{int64(13), int64(23)}, 4}
	if !reflect.DeepEqual(pool.queries[2].args, wantArgs) {
		t.Errorf("top-up args = %v, want [hungry zones, cursors, quota=remainder 4] = %v",
			pool.queries[2].args, wantArgs)
	}
}

// TestDispatchBatch_Empty — no active zones: the zones query is enough,
// the batch is empty without an error.
func TestDispatchBatch_Empty(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{dispatchZonesResult()}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000})
	if err != nil {
		t.Fatalf("an empty selection: %v", err)
	}
	if len(batch) != 0 {
		t.Errorf("batch = %+v, want empty", batch)
	}
	if len(pool.queries) != 1 {
		t.Errorf("queries = %d, want 1 (queues are not read without zones)", len(pool.queries))
	}
}

// TestDispatchBatch_Exhausted — all zones gave less than the quota: the batch
// is shorter than Limit (all pending is read), without a top-up.
func TestDispatchBatch_Exhausted(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(drow(1, 11, "a"), drow(2, 21, "b")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch = %+v, want 2 rows (pending exhausted)", batch)
	}
	if len(pool.queries) != 2 {
		t.Errorf("queries = %d, want 2 (no top-up needed: no zones with a tail)", len(pool.queries))
	}
}

// TestDispatchBatch_QuotaMissingZoneWeight — a zone outside the weight map
// participates in the round quota with weight 1 (not 0): the quota ceil(L × maxW / ΣW) + 1
// is computed over the normalized weights of all active zones.
func TestDispatchBatch_QuotaMissingZoneWeight(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(drow(1, 11, "a")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       7,
		ZoneWeights: map[int16]int{1: 5}, // zone 2 outside the map → weight 1
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || batch[0].DomainID != 11 {
		t.Fatalf("batch = %+v, want the single row of zone 1", batch)
	}

	// Quota ceil(7 × 5 / (5 + 1)) + 1 = 7 — the missing zone's weight equals 1.
	wantArgs := []any{[]any{int16(1), int16(2)}, []any{int64(0), int64(0)}, 7}
	if !reflect.DeepEqual(pool.queries[1].args, wantArgs) {
		t.Errorf("args = %v, want [zones, cursors, quota=7] = %v (a zone without a map weighs 1)",
			pool.queries[1].args, wantArgs)
	}
}

// TestDispatchBatch_FullBatchStopsRefill — the batch filled up in the top-up
// round: no new queries (the "batch full" break), the calls are —
// zones + queues + one top-up.
func TestDispatchBatch_FullBatchStopsRefill(t *testing.T) {
	r0 := append(drowIDs(1, 11, 12, 13, 14), drow(2, 31, "z"))
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		// Quota ceil(6 × 1 / 2) + 1 = 4: zone 1 is truncated (starved),
		// zone 2 gave everything (1 row).
		drows(r0),
		// Top-up of zone 1 (after id 14): the quota — the batch remainder
		// (6 − 5 = 1, T-183), LATERAL returns no more than the remainder.
		drows(drowIDs(1, 21)),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 6})
	if err != nil {
		t.Fatal(err)
	}
	// Zone 1 keys (weight 1): 0.5, 1.5, …; zone 2 — one row with key 0.5;
	// equal keys — the smaller zone_id.
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 31, ZoneID: 2, Name: "z.example.net"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
		{DomainID: 13, ZoneID: 1, Name: "d13.example.com"},
		{DomainID: 14, ZoneID: 1, Name: "d14.example.com"},
		{DomainID: 21, ZoneID: 1, Name: "d21.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v", batch, want)
	}
	if len(pool.queries) != 3 {
		t.Fatalf("queries = %d, want 3: a full batch stops the top-up", len(pool.queries))
	}
}

// TestDispatchBatch_RefillStopsAtExhaustedZone — an empty response of a starved
// zone in a top-up round does not stop the processing of the other zones:
// continue (not break) — the neighbor zone's tail enters the batch.
func TestDispatchBatch_RefillStopsAtExhaustedZone(t *testing.T) {
	r0 := append(drowIDs(1, 11, 12, 13, 14, 15, 16, 17, 18),
		drowIDs(2, 21, 22, 23, 24, 25, 26, 27, 28)...)
	r0 = append(r0, drow(3, 31, "z1"), drow(3, 32, "z2"), drow(3, 33, "z3"))
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net"), zrow(3, "example.org")),
		// Quota ceil(20 × 1 / 3) + 1 = 8: zones 1 and 2 are truncated (starved),
		// zone 3 gave everything (3 rows).
		drows(r0),
		// Top-up (the quota — the remainder 20 − 19 = 1): zone 1 is exhausted
		// (empty), zone 2 gives the tail.
		drows(drowIDs(2, 29)),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	// Round-robin order: keys (rn − 0.5)/1, equal ones — the smaller zone_id.
	wantIDs := []int64{11, 21, 31, 12, 22, 32, 13, 23, 33, 14, 24, 15, 25, 16, 26, 17, 27, 18, 28, 29}
	if len(batch) != len(wantIDs) {
		t.Fatalf("batch = %d rows, want %d (the tail of zone 2 from the top-up got in)", len(batch), len(wantIDs))
	}
	for i, id := range wantIDs {
		if batch[i].DomainID != id {
			t.Fatalf("batch[%d] = domain %d, want %d (round-robin order with weights 1)", i, batch[i].DomainID, id)
		}
	}
	if len(pool.queries) != 3 {
		t.Fatalf("queries = %d, want 3 (zones + queues + top-up)", len(pool.queries))
	}
}

// TestDispatchBatch_MergeSkipsEmptyZones — a zone without rows is skipped
// when building the merge heads (continue, not break): the rows of the following
// zones get into the batch.
func TestDispatchBatch_MergeSkipsEmptyZones(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(drow(2, 21, "b"), drow(2, 22, "c")), // zone 1 is empty
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 22, ZoneID: 2, Name: "c.example.net"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (an empty zone does not mute the merge)", batch, want)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("queries = %d, want 2 (no top-up: the zones are not hungry)", len(pool.queries))
	}
}

// TestDispatchBatch_MergeNormalizesMissingWeight — a zone's weight outside the map
// is normalized to 1 in the merge too: the key of the first row (1 − 0.5)/1 = 0.5 (not
// the +Inf of a zero weight and not 0.25 of weight 2) — the exact interleaving order
// with the weight-2 zone.
func TestDispatchBatch_MergeNormalizesMissingWeight(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(3, 31, 32)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net"), zrow(3, "example.org")),
		drows(rows), // zone 2 is empty
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       4,
		ZoneWeights: map[int16]int{3: 2}, // zones 1 and 2 outside the map → weight 1
	})
	if err != nil {
		t.Fatal(err)
	}
	// Zone 1 (weight 1): keys 0.5, 1.5; zone 3 (weight 2): 0.25, 0.75.
	want := []DispatchRow{
		{DomainID: 31, ZoneID: 3, Name: "d31.example.org"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 32, ZoneID: 3, Name: "d32.example.org"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (a zone without a map weighs 1)", batch, want)
	}
}

// TestDispatchBatch_MergeAdvanceKey — the zone head advancement key
// (rn − 0.5)/weight is recomputed with the same normalized weight: the second
// row of the zone without the map goes by key 1.5/1 (not the +Inf of a zero weight and not
// 0.75 of weight 2), interleaving with the tails of the weight-1 and weight-2 zones.
func TestDispatchBatch_MergeAdvanceKey(t *testing.T) {
	r0 := append(drowIDs(1, 11, 12), drowIDs(3, 31, 32, 33)...)
	r0 = append(r0, drowIDs(4, 41, 42, 43)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(3, "example.org"), zrow(4, "example.dev")),
		drows(r0),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       8,
		ZoneWeights: map[int16]int{3: 1, 4: 2}, // zone 1 outside the map → weight 1
	})
	if err != nil {
		t.Fatal(err)
	}
	// Keys: zone 1 — 0.5, 1.5; zone 3 (weight 1) — 0.5, 1.5, 2.5;
	// zone 4 (weight 2) — 0.25, 0.75, 1.25; equal ones — the smaller zone_id.
	want := []DispatchRow{
		{DomainID: 41, ZoneID: 4, Name: "d41.example.dev"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 31, ZoneID: 3, Name: "d31.example.org"},
		{DomainID: 42, ZoneID: 4, Name: "d42.example.dev"},
		{DomainID: 43, ZoneID: 4, Name: "d43.example.dev"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
		{DomainID: 32, ZoneID: 3, Name: "d32.example.org"},
		{DomainID: 33, ZoneID: 3, Name: "d33.example.org"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (the progress key with the normalized weight)", batch, want)
	}
}

// TestDispatchBatch_MergeKeyFormula — the head key (rn − 0.5)/weight: the first
// row of the weight-2 zone outranks the weight-1 zone's row; the second one (1.5/2 = 0.75)
// overtakes its second (1.5/1) — the exact interleaving pattern of weights 1 and 2.
func TestDispatchBatch_MergeKeyFormula(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(2, 21, 22)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		drows(rows),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       4,
		ZoneWeights: map[int16]int{1: 1, 2: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Zone 1 keys: 0.5, 1.5; zone 2 (weight 2): 0.25, 0.75.
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "d21.example.net"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 22, ZoneID: 2, Name: "d22.example.net"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v ((rn − 0.5)/weight)", batch, want)
	}
}

// TestDispatchBatch_MergeAdvanceShift — subtracting exactly 0.5 in the
// advancement key (rn − 0.5)/weight: a shift of +1/weight (the +0.5 mutant) changes
// the interleaving — the second row of the weight-4 zone (key 1.5/4 = 0.375) goes before
// the first row of the weight-1 zone (0.5), and with the shift (2.5/4 = 0.625) — after;
// weight 4 against 1 gives a substantial difference of steps.
func TestDispatchBatch_MergeAdvanceShift(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(2, 21, 22, 23, 24, 25, 26)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		drows(rows),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       8,
		ZoneWeights: map[int16]int{1: 1, 2: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Zone 1 keys: 0.5, 1.5; zone 2 (weight 4): 0.125, 0.375, 0.625, 0.875,
	// 1.125, 1.375 — zone 2 takes 6 of the batch's 8 places.
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "d21.example.net"},
		{DomainID: 22, ZoneID: 2, Name: "d22.example.net"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 23, ZoneID: 2, Name: "d23.example.net"},
		{DomainID: 24, ZoneID: 2, Name: "d24.example.net"},
		{DomainID: 25, ZoneID: 2, Name: "d25.example.net"},
		{DomainID: 26, ZoneID: 2, Name: "d26.example.net"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (the progress key shift — 0.5)", batch, want)
	}
}

// TestDispatchBatch_MergeTieSmallerZoneFirst — equal keys (equal rn and
// weights) are resolved by the smaller zone_id, even when the zone with the larger id comes
// first in the reference table.
func TestDispatchBatch_MergeTieSmallerZoneFirst(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		// The reference table order: zone 2 before zone 1.
		dispatchZonesResult(zrow(2, "example.net"), zrow(1, "example.com")),
		dispatchDomainsResult(drow(1, 11, "a"), drow(2, 21, "b")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 21, ZoneID: 2, Name: "b.example.net"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (the tie-break is the smaller zone_id)", batch, want)
	}
}

// TestDispatchBatch_MergeKeyBeatsZoneID — the key beats zone_id: the head
// with the smaller key is taken earlier, even if its zone is larger by id and
// later in heads (the tie-break applies only on key equality).
func TestDispatchBatch_MergeKeyBeatsZoneID(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		// The reference table order: zone 2 (weight 3, key 1/6) before zone 1 (0.5).
		dispatchZonesResult(zrow(2, "example.net"), zrow(1, "example.com")),
		dispatchDomainsResult(drow(1, 11, "a"), drow(2, 21, "b")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       2,
		ZoneWeights: map[int16]int{2: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 11, ZoneID: 1, Name: "a.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (the smaller key first)", batch, want)
	}
}

// TestDispatchBatch_MergeLimitCap — the merge stops exactly at
// Limit: the "extra" read rows (the quota headroom) do not get into the batch.
func TestDispatchBatch_MergeLimitCap(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(2, 21, 22)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		// Quota ceil(2 × 1 / 2) + 1 = 2: both zones are "starved", twice
		// more than Limit is read.
		drows(rows),
		dispatchDomainsResult(), // top-up (not executed on the original)
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 21, ZoneID: 2, Name: "d21.example.net"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (Limit = 2)", batch, want)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("queries = %d, want 2: a full batch — no top-up", len(pool.queries))
	}
}

// TestDispatchBatch_LimitOne — Limit = 1 is valid (the lower bound):
// a batch of one row.
func TestDispatchBatch_LimitOne(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com")),
		dispatchDomainsResult(drow(1, 11, "a")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{{DomainID: 11, ZoneID: 1, Name: "a.example.com"}}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v", batch, want)
	}
}

// TestDispatchBatch_ZonesIterError — an error of iterating the zone reference table
// (rows.Err after the rows) interrupts the selection with a wrapped "active
// zones" error.
func TestDispatchBatch_ZonesIterError(t *testing.T) {
	boom := errors.New("the iteration failed")
	pool := &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, err: boom},
	}}

	_, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "active zones") {
		t.Fatalf("err = %v, want the wrapped zone reference table iteration error", err)
	}
}

func TestDispatchBatch_Validation(t *testing.T) {
	cases := []struct {
		name string
		q    DispatchQuery
		want string
	}{
		{"limit 0", DispatchQuery{}, "Limit = 0"},
		{"a negative limit", DispatchQuery{Limit: -5}, "Limit = -5"},
		{"a zero weight", DispatchQuery{Limit: 1000, ZoneWeights: map[int16]int{7: 0}}, "zone 7 weight = 0"},
		{"a negative weight", DispatchQuery{Limit: 1000, ZoneWeights: map[int16]int{7: -2}}, "zone 7 weight = -2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &dispatchPool{}
			_, err := DispatchBatch(context.Background(), pool, tc.q)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want the description with %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "scheduler:") {
				t.Errorf("message = %q, want the \"scheduler:\" context", err)
			}
			if len(pool.queries) != 0 {
				t.Errorf("the invalid query reached the DB: %v", pool.queries)
			}
		})
	}
}

func TestDispatchBatch_QueryError(t *testing.T) {
	boom := errors.New("connection lost")

	// The zones query error.
	pool := &dispatchPool{err: boom}
	if _, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000}); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "active zones") {
		t.Fatalf("err = %v, want the wrapped zones query error", err)
	}

	// The queues query error (the zones are read): the failure is injected by the second
	// call, the message — without the zones context (unlike the error above).
	pool = &dispatchPool{
		results: []*dispatchRows{dispatchZonesResult(zrow(1, "example.com"))},
		errs:    []error{nil, boom},
	}
	if _, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000}); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "scheduler: domain batch selection") ||
		strings.Contains(err.Error(), "active zones") {
		t.Fatalf("err = %v, want the queues query wrapped \"connection lost\"", err)
	}
}

// TestDispatchBatch_ReadErrors — a row read error and an iteration error
// interrupt the selection with a wrapped error (both queries).
func TestDispatchBatch_ReadErrors(t *testing.T) {
	boom := errors.New("the decoding failed")

	pool := &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, scanErr: boom},
	}}
	_, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "active zones") {
		t.Fatalf("err = %v, want the wrapped zones read error", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com")),
		{colNames: dispatchDomainCols, rows: [][]any{drow(1, 1, "a"), drow(1, 2, "b")}, scanErr: boom},
	}}
	_, err = DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "row read") {
		t.Fatalf("err = %v, want the wrapped row read error", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com")),
		{colNames: dispatchDomainCols, rows: [][]any{drow(1, 1, "a")}, err: boom},
	}}
	if _, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10}); err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the iteration error (rows.Err)", err)
	}
}

// TestDispatchBatch_ContextCanceled — a cancelled context is refused at the
// very first DB call (like real pgx).
func TestDispatchBatch_ContextCanceled(t *testing.T) {
	pool := &dispatchPool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := DispatchBatch(ctx, pool, DispatchQuery{Limit: 1000})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(pool.queries) != 0 {
		t.Errorf("there must be no DB calls, got %v", pool.queries)
	}
}

// --- zone weights from the configuration (T-106) ---------------------------------

// TestResolveZoneWeights — zone names (the scheduler.zone_weights keys)
// are resolved to zone_id via the reference table in one query: all zones, not
// only active ones (an inactive zone's weight takes effect when it is enabled);
// extra reference table rows — off-map; names without a row — unknown
// ascending (the caller's Warn); the caller's map is not modified.
func TestResolveZoneWeights(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(
			zrow(2, "example.net"), // active or not — the whole reference table
			zrow(1, "example.com"),
			zrow(3, "example.org"),
		),
	}}
	byName := map[string]int{"example.com": 1, "example.net": 3, "example.org": 2,
		"zzz.missing": 7, "aaa.missing": 5}

	weights, unknown, err := ResolveZoneWeights(context.Background(), pool, byName)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int16]int{1: 1, 2: 3, 3: 2}
	if !reflect.DeepEqual(weights, want) {
		t.Errorf("weights = %v, want %v (names → zone_id via the reference table)", weights, want)
	}
	// Unknown names — ascending: a deterministic Warn.
	if want := []string{"aaa.missing", "zzz.missing"}; !reflect.DeepEqual(unknown, want) {
		t.Errorf("unknown = %v, want %v", unknown, want)
	}
	// The caller's map is not modified (the config lives its own life).
	if len(byName) != 5 || byName["example.net"] != 3 {
		t.Errorf("the caller's map was mutated: %v", byName)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != resolveZoneWeightsSQL {
		t.Errorf("queries = %v, want a single resolveZoneWeightsSQL", pool.queries)
	}

	// An empty map and nil — nil without touching the DB: equal weights,
	// the default behavior (spec 5.3: "optional").
	for _, m := range []map[string]int{nil, {}} {
		pool := &dispatchPool{}
		weights, unknown, err := ResolveZoneWeights(context.Background(), pool, m)
		if err != nil || unknown != nil || weights != nil {
			t.Fatalf("map %v: weights=%v unknown=%v err=%v, want nil, nil, nil", m, weights, unknown, err)
		}
		if len(pool.queries) != 0 {
			t.Errorf("map %v: there must be no DB calls, got %v", m, pool.queries)
		}
	}
}

// TestResolveZoneWeights_UnknownSorted — unknown names are returned
// ascending (a deterministic Warn of the caller): sort.Strings
// is mandatory — the map iteration order is randomized. One pass catches
// the removal of sorting only by chance (the iteration may coincide with the
// sorted order), hence several passes over the map of
// five names (the chance of one pass coinciding is ≤ 1/5!).
func TestResolveZoneWeights_UnknownSorted(t *testing.T) {
	byName := map[string]int{
		"m.example": 1, "c.example": 2, "a.example": 3, "x.example": 4, "k.example": 5,
	}
	want := []string{"a.example", "c.example", "k.example", "m.example", "x.example"}
	for i := 0; i < 12; i++ {
		pool := &dispatchPool{results: []*dispatchRows{
			dispatchZonesResult(zrow(1, "example.com")), // all names unknown
		}}
		_, unknown, err := ResolveZoneWeights(context.Background(), pool, byName)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(unknown, want) {
			t.Fatalf("pass %d: unknown = %v, want %v (ascending)", i, unknown, want)
		}
	}
}

// TestResolveZoneWeights_Errors — query/read/iteration errors
// interrupt the resolution with a wrapped error; a cancelled ctx does not reach the
// DB (like real pgx).
func TestResolveZoneWeights_Errors(t *testing.T) {
	boom := errors.New("the reference table is unavailable")
	weights := map[string]int{"example.com": 2}

	pool := &dispatchPool{err: boom}
	if _, _, err := ResolveZoneWeights(context.Background(), pool, weights); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "zone reference table") {
		t.Fatalf("err = %v, want the wrapped reference table query error", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, scanErr: boom},
	}}
	if _, _, err := ResolveZoneWeights(context.Background(), pool, weights); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "read") {
		t.Fatalf("err = %v, want the wrapped reference table read error", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, err: boom},
	}}
	if _, _, err := ResolveZoneWeights(context.Background(), pool, weights); err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the iteration error (rows.Err)", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool = &dispatchPool{}
	if _, _, err := ResolveZoneWeights(ctx, pool, weights); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(pool.queries) != 0 {
		t.Errorf("there must be no DB calls, got %v", pool.queries)
	}
}

// TestResolveZoneWeights_Dispatch — weights from the configuration reach the
// batch merge: names are resolved to zone_id, the selection with the obtained
// map interleaves the zones proportionally to the weights (modeled on TestDispatchBatch
// with ZoneWeights — the same expectation at zone 2 weight = 3).
func TestResolveZoneWeights_Dispatch(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")), // name resolution
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")), // selection
		dispatchDomainsResult(
			drow(2, 201, "b"),
			drow(1, 102, "c"),
			drow(1, 101, "a"),
		),
	}}

	weights, unknown, err := ResolveZoneWeights(context.Background(), pool, map[string]int{"example.net": 3})
	if err != nil || len(unknown) != 0 {
		t.Fatalf("weight resolution: unknown=%v err=%v", unknown, err)
	}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 3, ZoneWeights: weights})
	if err != nil {
		t.Fatal(err)
	}
	// Zone 2 weight = 3: its first row goes first (key 0.5/3 < 0.5/1),
	// then — interleaving by the key (rn − 0.5)/weight.
	want := []DispatchRow{
		{DomainID: 201, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 102, ZoneID: 1, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("batch = %+v, want %+v (configuration weights affect the merge)", batch, want)
	}
}

// ---------------------------------------------------------------- SQL contract

const wantDispatchZonesSQL = "SELECT id, name FROM zones WHERE is_active AND domains_pending > 0"

const wantDispatchZoneSQL = "SELECT d.zone_id, d.id, d.label " +
	"FROM unnest($1::int2[], $2::int8[]) AS z(zone_id, after_id) " +
	"JOIN LATERAL (" +
	"SELECT dd.zone_id, dd.id, dd.label FROM domains AS dd " +
	"WHERE dd.zone_id = z.zone_id AND dd.id > z.after_id " +
	"AND dd.check_state = 0 AND dd.status <> 6 " +
	"ORDER BY dd.id LIMIT $3" +
	") AS d ON true"

// TestDispatch_SQLContract — the exact texts of both queries and the reliance on
// the schema (migrations 000001/000016): reading a zone's queue over the partial
// index domains_pending_idx (the zone filter + ORDER BY id + LIMIT — without
// windows and sorting of all pending), code literals — from the model, the guard against
// deleted, zones — only active and only with work (domains_pending >
// 0, T-183: the T-178 column — its semantics is the dispatcher's predicate itself,
// a zone probe without pending would return 0 rows — circle 1 no longer does
// that).
func TestDispatch_SQLContract(t *testing.T) {
	if dispatchZonesSQL != wantDispatchZonesSQL {
		t.Errorf("dispatchZonesSQL = %q,\nwant %q", dispatchZonesSQL, wantDispatchZonesSQL)
	}
	if dispatchZoneSQL != wantDispatchZoneSQL {
		t.Errorf("dispatchZoneSQL = %q,\nwant %q", dispatchZoneSQL, wantDispatchZoneSQL)
	}

	// The literals in the text above must follow the model codes (spec 6.1,
	// 6.2): when the codes change the text is rebuilt automatically — the test catches
	// the desync of a hand-written literal.
	if int16(model.CheckStatePending) != 0 || int16(model.StatusDeleted) != 6 {
		t.Errorf("the model codes changed (pending=%d deleted=%d): update the literals in wantDispatchZoneSQL",
			int16(model.CheckStatePending), int16(model.StatusDeleted))
	}

	up := dispatchReadMigration(t, "000001_init.up.sql")

	// The zone queue — over the planner's ready partial index:
	// LATERAL reads (zone_id, id) WHERE check_state = 0 with a LIMIT.
	if !strings.Contains(up, "CREATE INDEX domains_pending_idx") ||
		!strings.Contains(up, "ON domains (zone_id, id) WHERE check_state = 0") {
		t.Error("migration 000001 has no domains_pending_idx (zone_id, id) WHERE check_state = 0 — the zone queue LATERAL selection relies on it (spec 6.1)")
	}
	// The active zones filter — a schema column, not an invention.
	if !strings.Contains(up, "is_active") {
		t.Error("migration 000001 has no zones.is_active — the active zones filter references the schema")
	}
	// The work filter — a T-178 counters column (migration 000016):
	// the dispatcher's filter relies on a live incremental counter.
	counters := dispatchReadMigration(t, "000016_zone_pipeline_counters.up.sql")
	if !strings.Contains(counters, "ADD COLUMN domains_pending bigint NOT NULL DEFAULT 0") {
		t.Error("migration 000016 has no zones.domains_pending — the \"only zones with work\" filter (T-183) references the T-178 counter")
	}
	// There are no weights in the schema — the only source of weights is DispatchQuery.
	if strings.Contains(up, "zones.weight") {
		t.Error("the schema gained a zones.weight column — weights can come from the DB instead of parameters")
	}
}

// wantResolveZoneWeightsSQL — resolving weight names from the configuration:
// the reference table in full (active and inactive zones — an inactive zone's weight
// takes effect when the zone is enabled without a scheduler restart).
const wantResolveZoneWeightsSQL = "SELECT id, name FROM zones"

// TestResolveZoneWeights_SQLContract — the exact query text and the reliance on
// the schema: the uniqueness of zones.name (migration 000001) makes the resolution
// "name → zone_id" unambiguous.
func TestResolveZoneWeights_SQLContract(t *testing.T) {
	if resolveZoneWeightsSQL != wantResolveZoneWeightsSQL {
		t.Errorf("resolveZoneWeightsSQL = %q,\nwant %q", resolveZoneWeightsSQL, wantResolveZoneWeightsSQL)
	}
	up := dispatchReadMigration(t, "000001_init.up.sql")
	if !strings.Contains(up, "name            text     NOT NULL UNIQUE") {
		t.Error("migration 000001 has no UNIQUE on zones.name — the weight name resolution relies on the uniqueness")
	}
}

// dispatchReadMigration — the SQL of a migration from the embedded set (its own copy
// of the internal/ingest helper: the file is self-sufficient, the shared test helpers
// are not shared between the parallel tasks T-030/T-031).
func dispatchReadMigration(t *testing.T, name string) string {
	t.Helper()
	raw, err := migrations.FS.ReadFile(name)
	if err != nil {
		t.Fatalf("reading %s from the embedded set: %v", name, err)
	}
	return string(raw)
}

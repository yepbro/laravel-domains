// Package queue — a task queue on Redis Streams with consumer groups
// (spec 4.2).
//
// Package contents:
//
//   - queue.go — stream names and creation of the Redis client;
//   - stream.go — the Queue interface (the transport is replaceable:
//     replacing it with RabbitMQ behind the same interface is allowed,
//     spec 4.2) and Delivery;
//   - message.go — stream message formats: only identifiers and minimal
//     data (the domain name), JSON in the single data field;
//   - redisq.go — the RedisQueue implementation: XADD, XREADGROUP ">",
//     XACK, lazy group creation (XGROUP CREATE MKSTREAM, BUSYGROUP —
//     not an error);
//   - reliab.go — delivery reliability (T-025): the MAXLEN truncation
//     policy (DefaultStreamMaxLen), the Processing pattern (XACK only
//     after the result is written to the DB), returning stalled entries
//     with XAUTOCLAIM and an attempt counter (Reliab.Claim) and moving
//     those that exhausted their attempts to q:dead.
//   - trim.go — freeing the acknowledged prefix of the streams (T-141,
//     spec 4.2 "Addition 2026-10-23"): XTRIM MINID by the oldest entry
//     that must remain (the group's oldest pending / last-delivered-id) —
//     the acknowledged history does not accumulate up to the MAXLEN cap
//     and the Redis budget (spec 3.2).
//   - consumers.go — consumer group hygiene (T-155): XGROUP DELCONSUMER
//     for strictly empty (pending = 0) and inactive (idle ≥
//     redis.consumer_idle) consumers accumulated after worker restarts;
//     pending entries are not touched (DELCONSUMER with pending loses
//     them forever).
//
// Backpressure by queue length — T-026. A live Redis is not used in unit
// tests (the queue integration tests — T-032).
package queue

import (
	"github.com/redis/go-redis/v9"
)

// Redis Streams stream names (spec 4.2).
const (
	StreamDNS            = "q:dns"             // a batch of domains (id + name) → dns-worker
	StreamHTTP           = "q:http"            // a domain with DNS-live subdomains → http-worker
	StreamRender         = "q:render"          // a result for render → render-worker
	StreamRenderFallback = "q:render-fallback" // a result that failed render on the main engine → chromium worker B (spec 4.3, FR-6)
	StreamAnalyze        = "q:analyze"         // a result for analysis → analyzer
	StreamDead           = "q:dead"            // tasks that exhausted their attempts (manual triage)
)

// allStreams — all the queue streams: the single source of the names;
// knownStream (queue membership) and workingStreams (freeing the
// acknowledged prefix, trim.go) are built from it — a new stream is added
// with one line and is immediately known to the whole queue.
var allStreams = []string{StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead}

// AllStreams — a copy of the full stream list of the queue (allStreams)
// for the observability consumers: the status/crawlerctl output list, the
// monitor's load report. One source instead of diverging copies (S-P3-1:
// the q:render-fallback of T-237 dropped out of the summaries while the
// lists were written by hand): a stream added to allStreams is
// automatically visible in all observability lists. Subsets with a
// different semantics (alertStreams — only the streams limited by the
// backpressure gate) stay with their owners. A copy, not a slice: the
// list is a value, the caller cannot change the package constants.
func AllStreams() []string {
	return append([]string(nil), allStreams...)
}

// NewClient creates a Redis client at the address addr (e.g.
// "127.0.0.1:6379"; the redis configuration section, T-006). The
// connection pool and timeouts — go-redis defaults. The client is passed
// to NewRedisQueue; closing it is the client owner's concern (the service
// framework).
func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}

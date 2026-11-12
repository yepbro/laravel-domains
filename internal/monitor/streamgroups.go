// Source of the worker-stream consumer group state for the frozen queue
// alert (T-162; the poisoned-task signature from T-159:
// XREADGROUP returns no entries — the group's last-delivered-id stands
// still while the backlog is non-zero; measurement §12 — every P0 was
// found manually precisely by such "quiet" standstills). The group depth
// is queue.GroupDepth: the same arithmetic as the backpressure gate
// (min(lag, XLEN) + pending, the clamp of the phantom lag after the
// undelivered trim — T-168) — the source must agree with the queue
// overflow alert on whether there is work to do.
//
// The data is XINFO GROUPS for each worker stream (group name, lag,
// pending, last-delivered-id) and XLEN for the clamp: the same window
// into go-redis as redisDepth in internal/queue, but through its own
// interface — the queue package does not pull alert checks into the gate
// API (the gate is a producer mechanism, the monitor is an observer,
// spec 5.9); the depth formula itself is shared (queue.GroupDepth) so
// that observation and the gate do not drift apart.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"

	"crawler/internal/queue"
)

// StreamGroupState is a snapshot of one consumer group of a worker stream.
type StreamGroupState struct {
	// Stream is the stream name (q:http).
	Stream string
	// Group is the group name (http-worker).
	Group string
	// Depth is the group queue depth: the unread backlog
	// (min(lag, XLEN) — the T-168 clamp) plus unacknowledged deliveries
	// (pending). The formula is queue.GroupDepth (shared with
	// BackpressureGate.Len).
	Depth int64
	// LastDeliveredID is the group's last-delivered-id from XINFO GROUPS:
	// the ID of the last entry delivered to consumers (XREADGROUP ">").
	// It moves only with new deliveries: a standing ID at Depth > 0 —
	// the queue is not being read. "0-0" — no deliveries ever happened.
	LastDeliveredID string
}

// StreamGroupsSource is a worker-stream group state source; an interface
// for substitution in unit tests (the real read is XINFO/XLEN from a
// live Redis, which tests do not have). StreamLener/RedisMemorySource
// style.
type StreamGroupsSource interface {
	// StreamGroups returns snapshots of all worker-stream groups.
	// Streams without groups (the stream does not exist or the consumers
	// were never created) are not included in the list: there is no one
	// whose delivery movement could be measured.
	// An error means "not measured", with the reason attached.
	StreamGroups(ctx context.Context) ([]StreamGroupState, error)
}

// redisGroupsCmd is a window into go-redis for stream groups: XINFO
// GROUPS and XLEN (narrow, like redisDepth in internal/queue);
// *redis.Client (queue.NewClient) and *redis.ClusterClient satisfy
// the interface.
type redisGroupsCmd interface {
	XLen(ctx context.Context, key string) *redis.IntCmd
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
}

// RedisStreamGroups is a StreamGroupsSource on top of XINFO GROUPS +
// XLEN of the same go-redis client that reads stream depths
// (cmd/monitor).
type RedisStreamGroups struct {
	// RDB is the Redis client; nil — a source error.
	RDB redisGroupsCmd
}

// compile-time: RedisStreamGroups is a StreamGroupsSource.
var _ StreamGroupsSource = RedisStreamGroups{}

// StreamGroups traverses the worker streams (alertStreams): a stream
// without groups (the key is missing — Redis answers with an error — or
// the groups were never created) contributes nothing, the depth of such
// a stream is the queue overflow alert's territory (Len reads XLEN).
// A read error on any stream stops the whole pass: unread states are not
// substituted with emptiness, otherwise a failure would falsely "revive"
// a frozen group (resetting the tracking in the queue_frozen FSM).
func (s RedisStreamGroups) StreamGroups(ctx context.Context) ([]StreamGroupState, error) {
	if s.RDB == nil {
		return nil, fmt.Errorf("monitor: группы стримов: клиент Redis не задан")
	}
	var out []StreamGroupState
	for _, stream := range alertStreams {
		groups, err := s.RDB.XInfoGroups(ctx, stream).Result()
		if err != nil {
			// The stream does not exist yet — Redis answers with a
			// no such key error (not an empty list): no groups, the stream
			// is not frozen (the same branch as BackpressureGate.Len).
			if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
				continue
			}
			return nil, fmt.Errorf("monitor: группы %s: %w", stream, err)
		}
		if len(groups) == 0 {
			continue
		}
		// XLEN — the upper bound of each group's unread backlog
		// (the T-168 clamp): undelivered entries live in the stream.
		xlen, err := s.RDB.XLen(ctx, stream).Result()
		if err != nil {
			return nil, fmt.Errorf("monitor: группы %s (XLEN): %w", stream, err)
		}
		for _, g := range groups {
			out = append(out, StreamGroupState{
				Stream: stream,
				Group:  g.Name,
				// −1 — the lag is undefined (the undelivered trim, Redis < 7.0):
				// GroupDepth counts only the exact pending.
				Depth:           queue.GroupDepth(g, xlen),
				LastDeliveredID: g.LastDeliveredID,
			})
		}
	}
	return out, nil
}

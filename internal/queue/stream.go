package queue

// Task queue contract, not tied to a transport protocol
// (spec 4.2: "Redis Streams with consumer groups (replacing it with
// RabbitMQ behind the Queue interface is allowed)"). The Redis Streams
// implementation — redisq.go, the message formats — message.go.

import "context"

// Queue — the task transport between services (spec 4.2): the scheduler
// (T-028) publishes messages, workers read them in batches through a
// consumer group and acknowledge processing. The methods do not expose
// Redis specifics so the backend can be replaced without changing the
// consumers.
//
// The delivery contract is at-least-once (spec 3.4): an entry read by
// Consume stays assigned to the group consumer until Ack confirms it;
// a worker crash or a connection loss leads to redelivery, so processing
// must be idempotent (all result records in the DB are idempotent per
// spec 3.4).
//
// Reading and acknowledging are separated deliberately: per spec 4.2,
// XACK runs only after the result is written to the DB. Workers build
// the cycle "Consume → processing and writing to the DB → Ack"; an
// unacknowledged entry comes back through redelivery.
type Queue interface {
	// Publish publishes the message msg to the stream stream and returns
	// the entry identifier. It is an error if the stream does not match
	// the message type (Message.Streams) or the values fail validation —
	// the message is not published.
	Publish(ctx context.Context, stream string, msg Message) (string, error)

	// Consume reads up to n entries of the stream not yet read by the
	// group, on behalf of the consumer consumer. The group group is
	// created automatically on the first read (from the start of the
	// stream). An empty result without an error — there are no new
	// entries (the wait interval expired); a canceled context interrupts
	// the wait.
	Consume(ctx context.Context, stream, group, consumer string, n int) ([]Delivery, error)

	// Ack acknowledges the entries with identifiers ids after processing
	// completes (per spec 4.2 — after the result is written to the DB).
	Ack(ctx context.Context, stream, group string, ids ...string) error
}

// Delivery — an entry delivered from a stream. The payload is the raw
// bytes of the data field (a JSON message, message.go); decoding is
// Decode by the message type of the stream:
//
//	batch, err := queue.Decode[queue.DNSBatch](d)
//
// The entry identifier is needed by the Ack acknowledgement and by the
// return of stuck messages (XAUTOCLAIM — T-025).
type Delivery struct {
	Stream  string // source stream
	ID      string // entry identifier (XACK; XAUTOCLAIM in T-025)
	Payload []byte // JSON message — the value of the entry's data field
}

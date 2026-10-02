package kafka

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CloudEvents attributes, carried as Kafka headers in binary content mode
// (design doc 8.3).
const (
	HeaderSpecVersion = "ce_specversion"
	HeaderID          = "ce_id"     // the consumers' deduplication key
	HeaderType        = "ce_type"   // for example payment.captured.v1
	HeaderSource      = "ce_source" // the producing service
	HeaderTime        = "ce_time"   // RFC 3339
	specVersion       = "1.0"
)

// Event is one message to publish.
type Event struct {
	Topic string
	// Key decides the partition: every event with the same key stays in
	// order (booking ID for booking and payment events).
	Key   string
	Value []byte
	// ID is the event's unique ID; a UUIDv7 is generated when empty. Retries
	// of the same event must reuse it so consumers can drop duplicates.
	ID     string
	Type   string
	Source string
	// Time defaults to now.
	Time time.Time
	// Headers are extra headers (traceparent, from Phase 3's tracing).
	Headers map[string]string
}

func (e Event) record() (*kgo.Record, error) {
	if e.Topic == "" || e.Key == "" || e.Type == "" || e.Source == "" {
		return nil, errors.New("kafka: an event needs a topic, key, type and source")
	}
	if e.ID == "" {
		e.ID = uuid.Must(uuid.NewV7()).String()
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	headers := []kgo.RecordHeader{
		{Key: HeaderSpecVersion, Value: []byte(specVersion)},
		{Key: HeaderID, Value: []byte(e.ID)},
		{Key: HeaderType, Value: []byte(e.Type)},
		{Key: HeaderSource, Value: []byte(e.Source)},
		{Key: HeaderTime, Value: []byte(e.Time.UTC().Format(time.RFC3339Nano))},
	}
	for k, v := range e.Headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return &kgo.Record{Topic: e.Topic, Key: []byte(e.Key), Value: e.Value, Headers: headers}, nil
}

// Message is one consumed record.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
	// Attempt is 1 on first delivery to the handler, 2 on the first retry,
	// and so on (within this process; a redelivery after a restart starts
	// again at 1).
	Attempt int
}

// ID is the event's CloudEvents ID, the deduplication key.
func (m Message) ID() string { return m.Headers[HeaderID] }

// Type is the event's CloudEvents type.
func (m Message) Type() string { return m.Headers[HeaderType] }

func messageOf(r *kgo.Record, attempt int) Message {
	h := make(map[string]string, len(r.Headers))
	for _, kv := range r.Headers {
		h[kv.Key] = string(kv.Value)
	}
	return Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
		Key: string(r.Key), Value: r.Value, Headers: h, Attempt: attempt}
}

// permanent marks an error that retrying cannot fix.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent wraps err so the consumer sends the message to the dead-letter
// topic at once instead of retrying (an undecodable payload, for example).
func Permanent(err error) error { return permanent{err: err} }

// IsPermanent reports whether err was wrapped by Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

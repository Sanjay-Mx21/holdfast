package kafka

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDLQ(t *testing.T) {
	tests := map[string]string{
		"holdfast.booking.v1": "holdfast.booking.dlq.v1",
		"holdfast.payment.v1": "holdfast.payment.dlq.v1",
		"some.topic":          "some.topic.dlq",
	}
	for in, want := range tests {
		if got := DLQ(in); got != want {
			t.Errorf("DLQ(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTopicsIncludeEveryDeadLetterTopic(t *testing.T) {
	topics := Topics()
	for _, base := range []string{TopicBooking, TopicPayment, TopicInventory} {
		if !slices.Contains(topics, base) || !slices.Contains(topics, DLQ(base)) {
			t.Errorf("Topics() lacks %s or its dead-letter topic: %v", base, topics)
		}
	}
	if len(topics) != 6 {
		t.Errorf("Topics() has %d topics, want 6: %v", len(topics), topics)
	}
}

func TestEventRecordCarriesCloudEventsHeaders(t *testing.T) {
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	r, err := Event{
		Topic: TopicPayment, Key: "booking-1", Value: []byte("payload"),
		ID: "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77", Type: "payment.captured.v1", Source: "payment-svc",
		Time: at, Headers: map[string]string{"traceparent": "00-abc-def-01"},
	}.record()
	if err != nil {
		t.Fatal(err)
	}
	m := messageOf(r, 1)
	want := map[string]string{
		HeaderSpecVersion: "1.0", HeaderID: "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77",
		HeaderType: "payment.captured.v1", HeaderSource: "payment-svc",
		HeaderTime: "2026-10-05T12:00:00Z", "traceparent": "00-abc-def-01",
	}
	for k, v := range want {
		if m.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, m.Headers[k], v)
		}
	}
	if m.Topic != TopicPayment || m.Key != "booking-1" || string(m.Value) != "payload" || m.ID() != want[HeaderID] || m.Type() != "payment.captured.v1" {
		t.Errorf("message = %+v", m)
	}
}

func TestEventDefaultsAndValidation(t *testing.T) {
	r, err := Event{Topic: TopicBooking, Key: "k", Type: "booking.created.v1", Source: "booking-svc"}.record()
	if err != nil {
		t.Fatal(err)
	}
	m := messageOf(r, 1)
	id, err := uuid.Parse(m.ID())
	if err != nil || id.Version() != 7 {
		t.Errorf("generated ID %q, want a UUIDv7", m.ID())
	}
	if _, err := time.Parse(time.RFC3339Nano, m.Headers[HeaderTime]); err != nil {
		t.Errorf("generated time %q: %v", m.Headers[HeaderTime], err)
	}
	for _, e := range []Event{
		{Key: "k", Type: "t", Source: "s"},
		{Topic: "t", Type: "t", Source: "s"},
		{Topic: "t", Key: "k", Source: "s"},
		{Topic: "t", Key: "k", Type: "t"},
	} {
		if _, err := e.record(); err == nil {
			t.Errorf("event %+v accepted, want an error", e)
		}
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("bad payload")
	err := fmt.Errorf("decode: %w", Permanent(base))
	if !IsPermanent(err) || !errors.Is(err, base) {
		t.Errorf("IsPermanent / errors.Is lost the wrapped error: %v", err)
	}
	if IsPermanent(base) {
		t.Error("a plain error reported as permanent")
	}
}

func TestBackoffDoublesWithJitterUpToTheCap(t *testing.T) {
	cc := ConsumerConfig{Group: "g", Topics: []string{"t"}, Backoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	if err := cc.defaults(); err != nil {
		t.Fatal(err)
	}
	c, err := NewConsumer(testConfig(), "test", cc, nil, nil, discard())
	if err != nil {
		t.Fatal(err)
	}
	for attempt, ceiling := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 5: time.Second, 30: time.Second} {
		for range 50 {
			d := c.backoff(attempt)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("backoff(%d) = %s, want between %s and %s", attempt, d, ceiling/2, ceiling)
			}
		}
	}
}

func TestConsumerConfigDefaults(t *testing.T) {
	if err := (&ConsumerConfig{Topics: []string{"t"}}).defaults(); err == nil {
		t.Error("a consumer without a group was accepted")
	}
	if err := (&ConsumerConfig{Group: "g"}).defaults(); err == nil {
		t.Error("a consumer without topics was accepted")
	}
	cc := ConsumerConfig{Group: "g", Topics: []string{"t"}}
	if err := cc.defaults(); err != nil {
		t.Fatal(err)
	}
	if cc.MaxAttempts != 5 || cc.RetryFor != 10*time.Minute || cc.Backoff != 100*time.Millisecond || cc.MaxBackoff != 5*time.Second || cc.MaxPollRecords != 500 {
		t.Errorf("defaults = %+v", cc)
	}
}

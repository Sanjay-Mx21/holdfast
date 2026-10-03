package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

// ReplayGroup is the default consumer group whose committed offsets record
// how far a dead-letter topic has been replayed.
const ReplayGroup = "holdfastctl-dlq-replay"

// ReplayOptions tune ReplayDLQ.
type ReplayOptions struct {
	Group  string // default ReplayGroup
	DryRun bool   // list only: nothing is published or committed
	Max    int    // at most this many messages (0: all)
}

// Replayed describes one dead-lettered message.
type Replayed struct {
	Partition int32
	Offset    int64
	Topic     string // the original topic it goes back to
	ID        string // ce_id
	Type      string // ce_type
	Reason    string // why it was dead-lettered
}

// ReplayDLQ publishes the messages of a dead-letter topic back to the topics
// they came from (runbook RB-3), from where the group last stopped up to the
// topic's end when the call starts. Each keeps its key, value and headers,
// its ce_id included, without the dlq_* headers: consumers deduplicate on
// ce_id, so replaying a message that was in fact applied changes nothing.
// Progress is committed per partition as it goes, so a second run replays
// only what arrived since.
func ReplayDLQ(ctx context.Context, cfg config.Kafka, dlqTopic string, o ReplayOptions) ([]Replayed, error) {
	if !strings.Contains(dlqTopic, ".dlq.") {
		return nil, fmt.Errorf("kafka: %q is not a dead-letter topic", dlqTopic)
	}
	if o.Group == "" {
		o.Group = ReplayGroup
	}
	kc, err := kgo.NewClient(clientOpts(cfg, "holdfastctl")...)
	if err != nil {
		return nil, err
	}
	defer kc.Close()
	adm := kadm.NewClient(kc)
	ends, err := adm.ListEndOffsets(ctx, dlqTopic)
	if err != nil {
		return nil, fmt.Errorf("kafka: end offsets of %s: %w", dlqTopic, err)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("kafka: end offsets of %s: %w", dlqTopic, err)
	}
	committed, err := adm.FetchOffsets(ctx, o.Group)
	if err != nil {
		return nil, fmt.Errorf("kafka: offsets of group %s: %w", o.Group, err)
	}

	start := map[int32]kgo.Offset{}
	end := map[int32]int64{}
	ends.Each(func(e kadm.ListedOffset) {
		from := int64(0)
		if c, ok := committed.Lookup(dlqTopic, e.Partition); ok && c.At >= 0 {
			from = c.At
		}
		if from < e.Offset {
			start[e.Partition] = kgo.NewOffset().At(from)
			end[e.Partition] = e.Offset
		}
	})
	if len(start) == 0 {
		return nil, nil
	}

	reader, err := kgo.NewClient(append(clientOpts(cfg, "holdfastctl"),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dlqTopic: start}))...)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var out []Replayed
	done := kadm.Offsets{}
	commit := func() error {
		if o.DryRun || len(done) == 0 {
			return nil
		}
		resp, err := adm.CommitOffsets(context.WithoutCancel(ctx), o.Group, done)
		if err == nil {
			err = resp.Error()
		}
		return err
	}
	for len(end) > 0 && (o.Max == 0 || len(out) < o.Max) {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		fetches := reader.PollFetches(pctx)
		cancel()
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 && !errors.Is(errs[0].Err, context.DeadlineExceeded) {
			return out, errors.Join(fmt.Errorf("kafka: read %s: %w", dlqTopic, errs[0].Err), commit())
		}
		if fetches.Empty() {
			return out, errors.Join(fmt.Errorf("kafka: %s: no messages before its end offsets after 10 s", dlqTopic), commit())
		}
		var stop error
		fetches.EachRecord(func(r *kgo.Record) {
			last, open := end[r.Partition]
			if stop != nil || !open || r.Offset >= last || (o.Max > 0 && len(out) >= o.Max) {
				return
			}
			rep, back := restore(r)
			if !o.DryRun {
				if err := reader.ProduceSync(ctx, back).FirstErr(); err != nil {
					stop = fmt.Errorf("kafka: republish %s/%d@%d to %s: %w", r.Topic, r.Partition, r.Offset, back.Topic, err)
					return
				}
			}
			out = append(out, rep)
			done.Add(kadm.Offset{Topic: dlqTopic, Partition: r.Partition, At: r.Offset + 1, LeaderEpoch: -1})
			if r.Offset+1 >= last {
				delete(end, r.Partition)
			}
		})
		if stop != nil {
			return out, errors.Join(stop, commit())
		}
	}
	return out, commit()
}

// restore turns a dead-lettered record back into the original message.
func restore(r *kgo.Record) (Replayed, *kgo.Record) {
	rep := Replayed{Partition: r.Partition, Offset: r.Offset}
	back := &kgo.Record{Key: r.Key, Value: r.Value}
	for _, h := range r.Headers {
		switch h.Key {
		case "dlq_topic":
			rep.Topic = string(h.Value)
		case "dlq_reason":
			rep.Reason = string(h.Value)
		case "dlq_partition", "dlq_offset", "dlq_group":
		default:
			back.Headers = append(back.Headers, h)
			switch h.Key {
			case HeaderID:
				rep.ID = string(h.Value)
			case HeaderType:
				rep.Type = string(h.Value)
			}
		}
	}
	if rep.Topic == "" { // not written by Consumer.deadLetter: derive it
		rep.Topic = strings.Replace(r.Topic, ".dlq.", ".", 1)
	}
	back.Topic = rep.Topic
	return rep, back
}

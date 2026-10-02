package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/queue"
)

func overview() queue.Overview {
	now := time.Date(2026, 10, 5, 12, 0, 30, 0, time.UTC)
	return queue.Overview{
		Status: queue.Status{
			EventID: "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77", State: queue.StateOpen,
			OpensAt: now.Add(-30 * time.Second), AdmittedUpTo: 2500, QueueSize: 40_000,
			UpdatedAt: now.Add(-200 * time.Millisecond),
		},
		Config: queue.EventConfig{
			OpensAt: now.Add(-30 * time.Second), AdmissionRate: 83, MaxSessions: 10_000, SessionTTL: 10 * time.Minute,
		},
		StoredState: queue.StateOpen, LeaderEpoch: 3, ActiveSessions: 2400, OnWorkList: true, Now: now,
	}
}

func TestPrintQueueStatus(t *testing.T) {
	tests := []struct {
		name   string
		change func(*queue.Overview)
		want   []string
	}{
		{"leading", func(*queue.Overview) {}, []string{
			"event 0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77",
			"state       OPEN, opened 2026-10-05T12:00:00Z",
			"queue       40000 in line, 2500 admitted (admittedUpTo)",
			"sessions    2400 active of 10000; 83 admissions/s; sessions last 10m0s",
			"leader      epoch 3; status document written 200ms ago",
			"work list   yes",
		}},
		{"before any leader", func(o *queue.Overview) {
			o.LeaderEpoch, o.UpdatedAt = 0, time.Time{}
			o.State, o.StoredState = queue.StatePre, queue.StatePre
			o.Config.OpensAt = o.Now.Add(time.Hour)
		}, []string{
			"state       PRE, opens 2026-10-05T13:00:30Z",
			"leader      no controller has led yet; status document not written yet (no leader)",
		}},
		{"past T0, not flipped yet", func(o *queue.Overview) { o.StoredState = queue.StatePre }, []string{
			"state       OPEN (stored PRE: T0 has passed and no join or opener has flipped it yet)",
		}},
		{"off the work list", func(o *queue.Overview) { o.OnWorkList = false }, []string{
			"work list   NO: no opener or admission controller serves this event",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := overview()
			tt.change(&o)
			var out bytes.Buffer
			printQueueStatus(&out, o)
			for _, w := range tt.want {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output lacks %q:\n%s", w, out.String())
				}
			}
		})
	}
}

func TestQueueStatusJSON(t *testing.T) {
	o := overview()
	data, err := json.Marshal(queueStatusJSON(o))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"state":"OPEN"`, `"leaderEpoch":3`, `"sessionTtl":"10m0s"`, `"updatedAt":"2026-10-05T12:00:29.8Z"`, `"onWorkList":true`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("JSON lacks %s: %s", want, data)
		}
	}
	o.UpdatedAt = time.Time{}
	data, _ = json.Marshal(queueStatusJSON(o))
	if !strings.Contains(string(data), `"updatedAt":null`) {
		t.Errorf("no leader yet must give updatedAt null: %s", data)
	}
}

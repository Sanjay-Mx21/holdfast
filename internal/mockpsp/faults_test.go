package mockpsp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFaultValidation(t *testing.T) {
	good := Faults{DuplicateRate: 0.2, DelayRate: 0.1, DelayMin: Duration(30 * time.Second), DelayMax: Duration(90 * time.Second), LossRate: 0.05, FailureRate: 0.1, TimeoutRate: 0.05}
	if err := good.Validate(); err != nil {
		t.Fatalf("the E3 faults: %v", err)
	}
	for name, f := range map[string]Faults{
		"rate above 1":     {LossRate: 1.5},
		"negative rate":    {DuplicateRate: -0.1},
		"min above max":    {DelayMin: Duration(time.Minute), DelayMax: Duration(time.Second)},
		"delay too long":   {DelayMax: Duration(time.Hour)},
		"timeout too long": {TimeoutDelay: Duration(time.Hour)},
	} {
		if f.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestFaultsJSON(t *testing.T) {
	var f Faults
	if err := json.Unmarshal([]byte(`{"delayRate":0.5,"delayMin":"1s","delayMax":"2s"}`), &f); err != nil {
		t.Fatal(err)
	}
	if f.DelayMin.D() != time.Second || f.DelayMax.D() != 2*time.Second {
		t.Fatalf("decoded %+v", f)
	}
	b, _ := json.Marshal(Faults{TimeoutDelay: Duration(10 * time.Second)})
	if want := `"timeoutDelay":"10s"`; !json.Valid(b) || !strings.Contains(string(b), want) {
		t.Fatalf("encoded %s, want %s", b, want)
	}
	if err := json.Unmarshal([]byte(`{"delayMin":30}`), &f); err == nil {
		t.Fatal("a number accepted as a duration")
	}
}

func TestInjectorDecisions(t *testing.T) {
	in := NewInjector(1)
	for range 1000 {
		if in.Duplicate() || in.Lose() || in.Fail() || in.Timeout() != 0 || in.Delay() != 0 || in.Outage() {
			t.Fatal("a fault fired with none set")
		}
	}
	if err := in.Set(Faults{DuplicateRate: 1, LossRate: 1, FailureRate: 1, TimeoutRate: 1, DelayRate: 1, DelayMin: Duration(time.Second), DelayMax: Duration(3 * time.Second), Outage: true}); err != nil {
		t.Fatal(err)
	}
	for range 1000 {
		d := in.Delay()
		if !in.Duplicate() || !in.Lose() || !in.Fail() || in.Timeout() != 10*time.Second || d < time.Second || d > 3*time.Second || !in.Outage() {
			t.Fatalf("a certain fault did not fire (delay %s)", d)
		}
	}
	// Rates are honoured, and the same seed makes the same decisions.
	a, b := NewInjector(42), NewInjector(42)
	_ = a.Set(Faults{LossRate: 0.25})
	_ = b.Set(Faults{LossRate: 0.25})
	lost := 0
	for i := range 10_000 {
		x, y := a.Lose(), b.Lose()
		if x != y {
			t.Fatalf("decision %d differs between equal seeds", i)
		}
		if x {
			lost++
		}
	}
	if lost < 2300 || lost > 2700 {
		t.Fatalf("%d of 10000 lost at rate 0.25", lost)
	}
}

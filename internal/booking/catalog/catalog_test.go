package catalog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCreateValidatesBeforeTouchingTheDatabase(t *testing.T) {
	ok := NewEvent{Name: "Coldplay, Mumbai", SaleOpensAt: time.Now(), PerUserLimit: 4, UnitPricePaise: 250000, Capacity: 1000}
	for name, mutate := range map[string]func(*NewEvent){
		"empty name":     func(e *NewEvent) { e.Name = "  " },
		"long name":      func(e *NewEvent) { e.Name = strings.Repeat("x", 201) },
		"no sale time":   func(e *NewEvent) { e.SaleOpensAt = time.Time{} },
		"per-user limit": func(e *NewEvent) { e.PerUserLimit = 11 },
		"free ticket":    func(e *NewEvent) { e.UnitPricePaise = 0 },
		"no capacity":    func(e *NewEvent) { e.Capacity = 0 },
	} {
		e := ok
		mutate(&e)
		// A nil pool would panic if it were used.
		if _, err := Create(context.Background(), nil, e); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: got %v, want ErrInvalid", name, err)
		}
	}
}

//go:build integration

package catalog_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

func TestCreateGetDelete(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx := context.Background()
	opens := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	id, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "Coldplay, Mumbai", SaleOpensAt: opens, PerUserLimit: 4, UnitPricePaise: 250000, Capacity: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id.Version() != 7 {
		t.Fatalf("event IDs should be UUIDv7, got version %d", id.Version())
	}
	e, err := catalog.Get(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if e.Name != "Coldplay, Mumbai" || e.Capacity != 1000 || e.Sold != 0 || e.Status != "SCHEDULED" || !e.SaleOpensAt.Equal(opens) {
		t.Fatalf("event %+v", e)
	}
	if err := catalog.Delete(ctx, pool, id); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Get(ctx, pool, id); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("after delete: got %v, want ErrNotFound", err)
	}
	if _, err := catalog.Get(ctx, pool, uuid.New()); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("unknown event: got %v", err)
	}
}

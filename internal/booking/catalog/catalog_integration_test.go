//go:build integration

package catalog_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
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

func TestPublicAPI(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx := context.Background()
	opens := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	verified := opens.Add(15 * time.Minute)
	id, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "Public API test", SaleOpensAt: opens, VerifiedWindowEndsAt: &verified,
		PerUserLimit: 2, UnitPricePaise: 99900, Capacity: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	old, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "Long gone", SaleOpensAt: time.Now().Add(-30 * 24 * time.Hour), PerUserLimit: 1, UnitPricePaise: 100, Capacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = catalog.Delete(context.Background(), pool, id)
		_ = catalog.Delete(context.Background(), pool, old)
	})
	router := httpx.NewRouter()
	catalog.NewHandler(pool).Register(router)
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}

	rec := get("/v1/events/" + id.String())
	var e catalog.Public
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rec.Code, rec.Body.String())
	}
	if e.EventID != id.String() || e.Name != "Public API test" || !e.SaleOpensAt.Equal(opens) || e.PerUserLimit != 2 ||
		e.UnitPricePaise != 99900 || e.Capacity != 50 || e.VerifiedOnlyUntil == nil || !e.VerifiedOnlyUntil.Equal(verified) || e.AgentLockoutUntil != nil {
		t.Fatalf("event %+v", e)
	}
	if rec.Header().Get("Cache-Control") != "public, max-age=60" {
		t.Fatalf("Cache-Control %q", rec.Header().Get("Cache-Control"))
	}
	if rec := get("/v1/events/" + uuid.NewString()); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown event: %d", rec.Code)
	}
	if rec := get("/v1/events/nope"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad ID: %d", rec.Code)
	}

	rec = get("/v1/events")
	var list struct {
		Events []catalog.Public `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	found := false
	for _, ev := range list.Events {
		if ev.EventID == old.String() {
			t.Fatal("an event whose sale opened a month ago is listed")
		}
		found = found || ev.EventID == id.String()
	}
	if !found && len(list.Events) < catalog.ListLimit {
		t.Fatalf("the upcoming event is missing from %d events", len(list.Events))
	}
}

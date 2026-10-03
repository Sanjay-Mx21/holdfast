package catalog

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
)

// Public is the catalog as buyers see it: what is on sale and when. Live
// state (the queue, units left) comes from queue-svc's status document and
// inventory-svc's availability, never from here.
type Public struct {
	EventID           string     `json:"eventId"`
	Name              string     `json:"name"`
	SaleOpensAt       time.Time  `json:"saleOpensAt"`
	VerifiedOnlyUntil *time.Time `json:"verifiedOnlyUntil,omitempty"`
	AgentLockoutUntil *time.Time `json:"agentLockoutUntil,omitempty"`
	PerUserLimit      int        `json:"perUserLimit"`
	UnitPricePaise    int64      `json:"unitPricePaise"`
	Capacity          int        `json:"capacity"`
}

func (e Event) public() Public {
	return Public{
		EventID: e.ID.String(), Name: e.Name, SaleOpensAt: e.SaleOpensAt.UTC(),
		VerifiedOnlyUntil: utc(e.VerifiedWindowEndsAt), AgentLockoutUntil: utc(e.AgentLockoutEndsAt),
		PerUserLimit: e.PerUserLimit, UnitPricePaise: e.UnitPricePaise, Capacity: e.Capacity,
	}
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// ListLimit is the most events List returns.
const ListLimit = 50

// List returns the events whose sale opened at most a week ago or has not
// opened yet, at most ListLimit: sales still to open first, soonest first,
// then sales already open, the most recently opened first.
func List(ctx context.Context, db *pgxpool.Pool) ([]Event, error) {
	rows, err := db.Query(ctx, `
		SELECT e.id, e.name, e.sale_opens_at, e.verified_window_ends_at, e.agent_lockout_ends_at,
		       e.per_user_limit, e.unit_price_paise, e.status, i.capacity, i.sold
		FROM booking.events e
		JOIN booking.event_inventory i ON i.event_id = e.id
		WHERE e.sale_opens_at > now() - interval '7 days'
		ORDER BY e.sale_opens_at <= now(),
		         CASE WHEN e.sale_opens_at > now() THEN e.sale_opens_at END,
		         e.sale_opens_at DESC, e.id
		LIMIT $1`, ListLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.Name, &e.SaleOpensAt, &e.VerifiedWindowEndsAt, &e.AgentLockoutEndsAt,
			&e.PerUserLimit, &e.UnitPricePaise, &e.Status, &e.Capacity, &e.Sold); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Handler serves the catalog's public API. Responses may be cached for a
// minute: events change rarely, and the edge absorbs a surge of page loads.
type Handler struct{ db *pgxpool.Pool }

// NewHandler returns a Handler reading db.
func NewHandler(db *pgxpool.Pool) *Handler { return &Handler{db: db} }

// Register mounts the routes on public. They need no identity.
func (h *Handler) Register(public *httpx.Router) {
	public.Handle("GET /v1/events", http.HandlerFunc(h.list))
	public.Handle("GET /v1/events/{eventID}", http.HandlerFunc(h.get))
}

const cacheControl = "public, max-age=60"

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	events, err := List(r.Context(), h.db)
	if err != nil {
		internal(w, r, err)
		return
	}
	out := make([]Public, 0, len(events))
	for _, e := range events {
		out = append(out, e.public())
	}
	w.Header().Set("Cache-Control", cacheControl)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"events": out})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("eventID"))
	if err != nil {
		httpx.WriteProblem(w, r, httpx.BadRequest("INVALID_REQUEST", "eventId must be a UUID"))
		return
	}
	e, err := Get(r.Context(), h.db, id)
	if errors.Is(err, ErrNotFound) {
		httpx.WriteProblem(w, r, httpx.NotFound("EVENT_NOT_FOUND", "no such event"))
		return
	}
	if err != nil {
		internal(w, r, err)
		return
	}
	w.Header().Set("Cache-Control", cacheControl)
	httpx.WriteJSON(w, http.StatusOK, e.public())
}

func internal(w http.ResponseWriter, r *http.Request, err error) {
	logging.FromContext(r.Context()).ErrorContext(r.Context(), "catalog: read failed", "err", err)
	httpx.WriteProblem(w, r, httpx.Internal())
}

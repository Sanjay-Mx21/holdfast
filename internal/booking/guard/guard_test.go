package guard

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestReserveValidatesBeforeTouchingTheDatabase(t *testing.T) {
	for _, r := range []Reservation{
		{EventID: uuid.New(), UserID: uuid.New(), Qty: 0, PerUserLimit: 4},
		{EventID: uuid.New(), UserID: uuid.New(), Qty: 5, PerUserLimit: 4},
		{EventID: uuid.New(), UserID: uuid.New(), Qty: 1, PerUserLimit: 0},
	} {
		// A nil transaction would panic if it were used.
		if err := Reserve(context.Background(), nil, r); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%+v: got %v, want ErrInvalid", r, err)
		}
	}
}

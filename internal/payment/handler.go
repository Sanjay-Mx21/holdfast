package payment

import (
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
)

// maxWebhookBytes bounds a webhook body.
const maxWebhookBytes = 64 << 10

// WebhookHandler receives the provider's callbacks (design doc 9.8).
type WebhookHandler struct {
	svc       *Service
	secret    []byte
	tolerance time.Duration
	now       func() time.Time
}

// NewWebhookHandler verifies callbacks with secret, accepting timestamps
// within tolerance of now (5 minutes in the design).
func NewWebhookHandler(svc *Service, secret []byte, tolerance time.Duration) *WebhookHandler {
	return &WebhookHandler{svc: svc, secret: secret, tolerance: tolerance, now: time.Now}
}

// Register adds POST /v1/webhooks/psp.
func (h *WebhookHandler) Register(public *httpx.Router) {
	public.Handle("POST /v1/webhooks/psp", h)
}

// ServeHTTP verifies the signature in constant time, then applies the
// webhook. 2xx tells the provider to stop; anything else makes it retry,
// which the deduplication makes safe.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBytes))
	if err != nil {
		httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "webhook body too large"))
		return
	}
	if err := psp.Verify(h.secret, r.Header.Get(psp.HeaderTimestamp), r.Header.Get(psp.HeaderSignature), body, h.now(), h.tolerance); err != nil {
		h.svc.m.webhook("bad_signature", false)
		httpx.WriteProblem(w, r, httpx.Unauthorized("BAD_SIGNATURE", "webhook signature or timestamp did not verify"))
		return
	}
	if err := h.svc.ApplyWebhook(r.Context(), body); err != nil {
		logging.FromContext(r.Context()).ErrorContext(r.Context(), "payment: applying webhook failed; the provider will retry", "err", err)
		if errors.Is(err, errMissingEventRef) {
			httpx.WriteProblem(w, r, httpx.Internal())
			return
		}
		httpx.WriteProblem(w, r, httpx.Unavailable("try again", 1))
		return
	}
	w.WriteHeader(http.StatusOK)
}

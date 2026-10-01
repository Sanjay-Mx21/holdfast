package httpx

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// Problem is an RFC 9457 "problem details" body extended with a stable,
// machine-readable Code that clients can switch on.
type Problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail,omitempty"`
	Instance  string `json:"instance,omitempty"`
	RequestID string `json:"requestId,omitempty"`

	// RetryAfter, when positive, is sent as a Retry-After header in seconds.
	RetryAfter int `json:"-"`
}

// Error implements error so a *Problem can travel through error returns.
func (p *Problem) Error() string { return p.Code + ": " + p.Detail }

// NewProblem builds a problem whose type URI is derived from code.
func NewProblem(status int, code, detail string) *Problem {
	return &Problem{
		Type:   "/problems/" + strings.ToLower(strings.ReplaceAll(code, "_", "-")),
		Title:  http.StatusText(status),
		Status: status,
		Code:   code,
		Detail: detail,
	}
}

// BadRequest is a 400 problem.
func BadRequest(code, detail string) *Problem { return NewProblem(http.StatusBadRequest, code, detail) }

// Unauthorized is a 401 problem.
func Unauthorized(code, detail string) *Problem {
	return NewProblem(http.StatusUnauthorized, code, detail)
}

// Forbidden is a 403 problem.
func Forbidden(code, detail string) *Problem { return NewProblem(http.StatusForbidden, code, detail) }

// NotFound is a 404 problem.
func NotFound(code, detail string) *Problem { return NewProblem(http.StatusNotFound, code, detail) }

// Conflict is a 409 problem.
func Conflict(code, detail string) *Problem { return NewProblem(http.StatusConflict, code, detail) }

// Unprocessable is a 422 problem.
func Unprocessable(code, detail string) *Problem {
	return NewProblem(http.StatusUnprocessableEntity, code, detail)
}

// Internal is a 500 problem that deliberately reveals nothing about the cause.
func Internal() *Problem {
	return NewProblem(http.StatusInternalServerError, "INTERNAL", "an unexpected error occurred")
}

// Unavailable is a 503 problem asking the client to retry after some seconds.
func Unavailable(detail string, retryAfter int) *Problem {
	p := NewProblem(http.StatusServiceUnavailable, "UNAVAILABLE", detail)
	p.RetryAfter = retryAfter
	return p
}

// WriteProblem writes p as application/problem+json. p itself is not modified.
func WriteProblem(w http.ResponseWriter, r *http.Request, p *Problem) {
	out := *p
	if r != nil {
		out.Instance = r.URL.Path
		out.RequestID = RequestIDFrom(r.Context())
	}
	h := w.Header()
	h.Set("Content-Type", "application/problem+json")
	h.Set("Cache-Control", "no-store")
	if out.RetryAfter > 0 {
		h.Set("Retry-After", strconv.Itoa(out.RetryAfter))
	}
	w.WriteHeader(out.Status)
	_ = json.NewEncoder(w).Encode(out)
}

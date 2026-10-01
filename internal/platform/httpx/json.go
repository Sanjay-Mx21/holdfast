package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// DecodeJSON reads exactly one JSON object from the request body into dst.
// Unknown fields, trailing data, wrong content types and oversized bodies are
// rejected with a *Problem the caller can write as-is. Pair it with the
// BodyLimit middleware so the size limit is enforced while reading.
func DecodeJSON(r *http.Request, dst any) *Problem {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || mt != "application/json" {
			return NewProblem(http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json")
		}
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		var syntaxErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &maxErr):
			return NewProblem(http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE",
				fmt.Sprintf("request body must not exceed %d bytes", maxErr.Limit))
		case errors.Is(err, io.EOF):
			return BadRequest("EMPTY_BODY", "request body must not be empty")
		case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF):
			return BadRequest("MALFORMED_JSON", "request body is not valid JSON")
		case errors.As(err, &typeErr):
			return BadRequest("INVALID_FIELD_TYPE", fmt.Sprintf("field %q has the wrong type", typeErr.Field))
		default:
			return BadRequest("INVALID_BODY", strings.TrimPrefix(err.Error(), "json: "))
		}
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return BadRequest("TRAILING_DATA", "request body must contain a single JSON object")
	}
	return nil
}

// WriteJSON marshals v before writing anything, so an encoding failure can
// still produce a clean 500 instead of a half-written response.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		WriteProblem(w, nil, Internal())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

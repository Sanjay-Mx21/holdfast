package psp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"
)

// Webhook event types the provider sends.
const (
	EventPaymentCaptured = "payment.captured"
	EventPaymentFailed   = "payment.failed"
	EventOrderExpired    = "order.expired"
	EventRefundCompleted = "refund.completed"
)

// Webhook headers: the signature is HMAC-SHA256 over "<timestamp>.<body>",
// hex-encoded, so an old message cannot be replayed with a new timestamp.
const (
	HeaderTimestamp = "X-PSP-Timestamp" // Unix seconds
	HeaderSignature = "X-PSP-Signature"
)

// Webhook is the body of a provider callback.
type Webhook struct {
	ID          string    `json:"id"` // the provider's event ID: the dedup key
	Type        string    `json:"type"`
	OrderID     string    `json:"orderId"`
	PaymentID   string    `json:"paymentId,omitempty"`
	RefundID    string    `json:"refundId,omitempty"`
	AmountPaise int64     `json:"amountPaise"`
	Reason      string    `json:"reason,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
}

// ErrBadSignature means a webhook's signature or timestamp did not verify.
var ErrBadSignature = errors.New("psp: bad webhook signature")

// Sign returns the signature of body sent at ts (for mockpsp and tests).
func Sign(secret []byte, ts string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a webhook's signature in constant time, and that its
// timestamp is within tolerance of now (both ways).
func Verify(secret []byte, ts, signature string, body []byte, now time.Time, tolerance time.Duration) error {
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrBadSignature
	}
	if d := now.Sub(time.Unix(sec, 0)); d > tolerance || d < -tolerance {
		return ErrBadSignature
	}
	got, err := hex.DecodeString(signature)
	if err != nil {
		return ErrBadSignature
	}
	want, _ := hex.DecodeString(Sign(secret, ts, body))
	if !hmac.Equal(got, want) {
		return ErrBadSignature
	}
	return nil
}

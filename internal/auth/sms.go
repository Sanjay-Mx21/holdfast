package auth

import (
	"context"
	"sync"
	"time"
)

// Sender delivers a text message to a phone number.
type Sender interface {
	Send(ctx context.Context, phone, text string) error
}

// Message is a text the mock gateway delivered.
type Message struct {
	Phone  string    `json:"phone"`
	Text   string    `json:"text"`
	SentAt time.Time `json:"sentAt"`
}

// Inbox is the mock SMS gateway (design doc section 3: "a mock phone OTP").
// It keeps the last message per number in memory, so a developer, a test or
// the demo can read the code (the development inbox, DEV_SMS_INBOX). It
// never logs a message: codes must not reach logs.
type Inbox struct {
	mu   sync.Mutex
	last map[string]Message
	max  int
	now  func() time.Time
}

// NewInbox keeps the last message of at most max numbers; when it is full,
// it starts again from empty (it is a development tool, not a store).
func NewInbox(max int) *Inbox {
	return &Inbox{last: map[string]Message{}, max: max, now: time.Now}
}

// Send implements Sender.
func (i *Inbox) Send(_ context.Context, phone, text string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.last) >= i.max {
		i.last = map[string]Message{}
	}
	i.last[phone] = Message{Phone: phone, Text: text, SentAt: i.now().UTC()}
	return nil
}

// Last returns the last message sent to phone (E.164).
func (i *Inbox) Last(phone string) (Message, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	m, ok := i.last[phone]
	return m, ok
}

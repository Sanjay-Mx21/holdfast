package inventory

import "strings"

// Every key of one event shares the hash tag {eventID}, so multi-key scripts
// run on a single Valkey Cluster slot (and a single shard).
type keys struct{ event string }

func keysFor(eventID string) keys { return keys{event: eventID} }

func (k keys) prefix() string            { return "inv:{" + k.event + "}:" }
func (k keys) avail() string             { return k.prefix() + "avail" }
func (k keys) config() string            { return k.prefix() + "config" }
func (k keys) expiry() string            { return k.prefix() + "expiry" }
func (k keys) user(userID string) string { return k.prefix() + "user:" + userID }
func (k keys) hold(holdID string) string { return k.prefix() + "hold:" + holdID }

// eventsKey is the set of provisioned events: the sweeper's work list.
const eventsKey = "inv:events"

// expiryMember encodes the expiry-index member. Carrying the user ID lets the
// sweeper build every key release.lua needs without an extra round trip.
func expiryMember(holdID, userID string) string { return holdID + "|" + userID }

func parseExpiryMember(m string) (holdID, userID string, ok bool) {
	holdID, userID, ok = strings.Cut(m, "|")
	return holdID, userID, ok && holdID != "" && userID != ""
}

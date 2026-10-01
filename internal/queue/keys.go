package queue

// Every key of one event shares the hash tag {eventID}, so multi-key scripts
// run on a single Valkey Cluster slot (and a single shard).
type keys struct{ event string }

func keysFor(eventID string) keys { return keys{event: eventID} }

func (k keys) prefix() string  { return "q:{" + k.event + "}:" }
func (k keys) config() string  { return k.prefix() + "config" }
func (k keys) state() string   { return k.prefix() + "state" }
func (k keys) members() string { return k.prefix() + "members" }
func (k keys) seq() string     { return k.prefix() + "seq" }

// eventsKey is the set of provisioned events: the opener's work list. It is
// one global key, never used inside multi-key scripts.
const eventsKey = "q:events"

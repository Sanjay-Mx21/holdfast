package queue

// Every key of one event shares the hash tag {eventID}, so multi-key scripts
// run on a single Valkey Cluster slot (and a single shard).
type keys struct{ event string }

func keysFor(eventID string) keys { return keys{event: eventID} }

func (k keys) prefix() string { return "q:{" + k.event + "}:" }
func (k keys) config() string { return k.prefix() + "config" }
func (k keys) state() string  { return k.prefix() + "state" }

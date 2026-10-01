package queue

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"
)

// lotteryScore returns a uniformly random number in [0, 1) with 53 bits of
// entropy, formatted so it round-trips exactly through Valkey. It uses
// crypto/rand, never math/rand: a predictable lottery could be gamed.
func lotteryScore() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	u := binary.BigEndian.Uint64(b[:]) >> 11 // keep 53 bits: the float64 mantissa size
	f := float64(u) / float64(uint64(1)<<53)
	return strconv.FormatFloat(f, 'g', 17, 64), nil
}

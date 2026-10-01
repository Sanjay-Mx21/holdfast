package authn

import (
	"crypto/ed25519"
	"encoding/base64"
)

// JWK is an Ed25519 public key as a JSON Web Key (RFC 8037: key type OKP).
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
}

// JWKSet is the document served at /.well-known/jwks.json.
type JWKSet struct {
	Keys []JWK `json:"keys"`
}

// PublicJWK encodes pub as a signing JWK, with the same key ID tokens carry.
func PublicJWK(pub ed25519.PublicKey) JWK {
	return JWK{
		Kty: "OKP",
		Crv: "Ed25519",
		X:   base64.RawURLEncoding.EncodeToString(pub),
		Kid: KeyID(pub),
		Alg: "EdDSA",
		Use: "sig",
	}
}

// NewJWKSet publishes keys, in order and without duplicates. During a key
// rotation it lists the new signing key and the old one, so verifiers keep
// accepting tokens signed by either.
func NewJWKSet(keys ...ed25519.PublicKey) JWKSet {
	set := JWKSet{Keys: make([]JWK, 0, len(keys))}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		j := PublicJWK(k)
		if seen[j.Kid] {
			continue
		}
		seen[j.Kid] = true
		set.Keys = append(set.Keys, j)
	}
	return set
}

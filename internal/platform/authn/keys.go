// Package authn verifies (and, for queue-svc and dev tooling, issues) the
// Ed25519-signed JWTs HoldFast uses between services, and provides HTTP
// middleware that authenticates requests with them.
package authn

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// GenerateKeyPair returns a new Ed25519 key pair as PEM (PKCS#8 private key,
// PKIX public key).
func GenerateKeyPair() (privPEM, pubPEM []byte, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), nil
}

// ParsePrivateKeyPEM decodes a PKCS#8 Ed25519 private key.
func ParsePrivateKeyPEM(data []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("authn: expected a PEM \"PRIVATE KEY\" block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("authn: parse private key: %w", err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("authn: private key is not Ed25519")
	}
	return priv, nil
}

// ParsePublicKeyPEM decodes a PKIX Ed25519 public key.
func ParsePublicKeyPEM(data []byte) (ed25519.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PUBLIC KEY" {
		return nil, errors.New("authn: expected a PEM \"PUBLIC KEY\" block")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("authn: parse public key: %w", err)
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("authn: public key is not Ed25519")
	}
	return pub, nil
}

// KeyID derives a stable key ID from a public key, so issuers and verifiers
// agree on "kid" without extra configuration and key rotation just works.
func KeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return base64.RawURLEncoding.EncodeToString(sum[:12])
}

// LoadPublicKeys reads PEM public keys from files and indexes them by KeyID.
// Listing both the old and new key during a rotation keeps tokens signed by
// either one valid.
func LoadPublicKeys(paths []string) (map[string]ed25519.PublicKey, error) {
	if len(paths) == 0 {
		return nil, errors.New("authn: no public key files configured")
	}
	keys := make(map[string]ed25519.PublicKey, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(filepath.Clean(p))
		if err != nil {
			return nil, fmt.Errorf("authn: read %s: %w", p, err)
		}
		pub, err := ParsePublicKeyPEM(data)
		if err != nil {
			return nil, fmt.Errorf("authn: %s: %w", p, err)
		}
		keys[KeyID(pub)] = pub
	}
	return keys, nil
}

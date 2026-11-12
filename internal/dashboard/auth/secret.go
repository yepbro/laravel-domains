// Package auth — storing and verifying the operator dashboard password
// (dashboard spec §7.2–7.4, T-127): the password_hash string format,
// generation (crawlerctl dashboard-secret), parsing and constant-time
// verification. The package knows nothing about HTTP: the Basic
// middleware and cookie sessions belong to cmd/dashboard (T-128);
// Derive — a KDF-derived key — is used to sign cookie sessions
// (dashboard spec §7.5: a password change invalidates sessions).
//
// The KDF is PBKDF2-HMAC-SHA256 from stdlib crypto/pbkdf2 (Go 1.24+,
// dashboard spec §3.2: no new dependencies). The string format
// (dashboard spec §7.2, b64 — std encoding):
//
//	pbkdf2-sha256$<iterations>$<salt-b64>$<key-b64>
package auth

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// Scheme parameters (dashboard spec §7.2).
const (
	// Scheme — the scheme name in the first component of the
	// password_hash string.
	Scheme = "pbkdf2-sha256"
	// DefaultIterations — the recommended number of iterations when
	// generating (OWASP for PBKDF2-HMAC-SHA256; dashboard spec §7.2).
	DefaultIterations = 600_000
	// MinSaltLen — the minimum salt in bytes (dashboard spec §7.2: ≥ 16).
	MinSaltLen = 16
	// KeyLen — the derived key length in bytes (dashboard spec §7.2: 32).
	KeyLen = 32
)

// Generate returns the password_hash string for a password:
// PBKDF2-HMAC-SHA256, a MinSaltLen-byte salt from crypto/rand, a
// KeyLen-byte key. iterations <= 0 means DefaultIterations (lower
// values are for tests). An empty password is an error: operator
// credentials are mandatory (dashboard spec §7.7).
func Generate(password string, iterations int) (string, error) {
	if password == "" {
		return "", fmt.Errorf("the password is empty")
	}
	if iterations <= 0 {
		iterations = DefaultIterations
	}
	salt := make([]byte, MinSaltLen)
	// Unreachable branch (T-M11): crypto/rand.Read in Go ≥ 1.24 does
	// not return errors — "never returns an error, and always fills b
	// entirely" (stdlib documentation); an entropy source failure is
	// a fatal process crash, not a Generate error.
	// mutator-disable-next-line branch/if
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("salt: %w", err)
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, iterations, KeyLen)
	// Unreachable branch (T-M11): pbkdf2.Key fails only when keyLength
	// is outside [1, (2^32-1)×h.Size()] or in a FIPS-only build
	// (GOEXPERIMENT=fips140 — not used by this project, and even there
	// a 16-byte salt and a 32-byte key pass the constraints), while
	// keyLength here is the KeyLen=32 constant; by this point the
	// iterations are already ≥ 1.
	// mutator-disable-next-line branch/if
	if err != nil {
		return "", fmt.Errorf("pbkdf2: %w", err)
	}
	return strings.Join([]string{
		Scheme,
		strconv.Itoa(iterations),
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(key),
	}, "$"), nil
}

// PasswordHash — a parsed password_hash string: the scheme parameters
// and the expected derived key. Immutability is not guaranteed — it is
// a copy of the value after Parse, not a defensive structure.
type PasswordHash struct {
	Iterations int
	Salt       []byte
	Key        []byte
}

// Parse parses a string in the dashboard spec §7.2 format. The format is
// strict: 4 components, the scheme name, positive integer iterations
// (read from the string — verification uses them, dashboard spec §7.2),
// a salt ≥ MinSaltLen bytes, a key of exactly KeyLen bytes, base64 std
// encoding. Errors are parsed per component — the text states what
// exactly is wrong.
func Parse(s string) (PasswordHash, error) {
	parts := strings.Split(s, "$")
	if len(parts) != 4 || parts[0] != Scheme {
		return PasswordHash{}, fmt.Errorf("a string of the form %s$<iterations>$<salt-b64>$<key-b64> is expected", Scheme)
	}
	iter, err := strconv.Atoi(parts[1])
	if err != nil || iter < 1 {
		return PasswordHash{}, fmt.Errorf("iterations: a positive integer is expected, got %q", parts[1])
	}
	salt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return PasswordHash{}, fmt.Errorf("salt: base64: %w", err)
	}
	if len(salt) < MinSaltLen {
		return PasswordHash{}, fmt.Errorf("salt: minimum %d bytes, got %d", MinSaltLen, len(salt))
	}
	key, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return PasswordHash{}, fmt.Errorf("key: base64: %w", err)
	}
	if len(key) != KeyLen {
		return PasswordHash{}, fmt.Errorf("key: exactly %d bytes, got %d", KeyLen, len(key))
	}
	return PasswordHash{Iterations: iter, Salt: salt, Key: key}, nil
}

// Derive — the password-derived key: the KDF with the hash parameters
// (iterations and salt from the string, KeyLen length). The same key
// signs the dashboard cookie session (dashboard spec §7.5). An error is
// possible only with negative parameters (unreachable after Parse).
func (h PasswordHash) Derive(password string) ([]byte, error) {
	return pbkdf2.Key(sha256.New, password, h.Salt, h.Iterations, KeyLen)
}

// Verify — constant-time password-to-hash verification
// (subtle.ConstantTimeCompare, dashboard spec §7.4). A derivation error
// (unreachable after Parse) counts as a mismatch.
func (h PasswordHash) Verify(password string) bool {
	key, err := h.Derive(password)
	// Unreachable branch (T-M11): Derive is pbkdf2.Key with keyLength =
	// the KeyLen=32 constant — the stdlib error conditions (keyLength
	// ≤ 0 or too long) are impossible for any h value in a normal
	// build; the only exception is a FIPS-only build
	// (GOEXPERIMENT=fips140, not used by this project) with a manually
	// constructed salt < 16 bytes — the production path through Parse
	// is excluded (Parse requires a salt ≥ MinSaltLen).
	// mutator-disable-next-line branch/if
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(key, h.Key) == 1
}

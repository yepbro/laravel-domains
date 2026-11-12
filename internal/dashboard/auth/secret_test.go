// Tests of dashboard password storage (dashboard spec §7.2–7.4): the
// "generate → parse → verify" roundtrip, a wrong password, strict
// format parsing, incompatible string parameters (iterations/key
// length).
package auth

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
)

// fastIterations — test iterations: the roundtrip checks the format
// and derivation, not the KDF cost (600,000 iterations ≈ half a
// second).
const fastIterations = 1000

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// Roundtrip: the generated string has the dashboard spec §7.2 shape,
// parses with the original parameters and verifies only against its
// own password.
func TestGenerateParseVerify(t *testing.T) {
	const password = "correct password s3cret!"
	hash, err := Generate(password, fastIterations)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	parts := strings.Split(hash, "$")
	if len(parts) != 4 {
		t.Fatalf("the hash %q: 4 $-separated components expected", hash)
	}
	if parts[0] != Scheme || parts[1] != "1000" {
		t.Errorf("the hash %q: %s$1000$… expected", hash, Scheme)
	}
	if salt, err := base64.StdEncoding.DecodeString(parts[2]); err != nil || len(salt) != MinSaltLen {
		t.Errorf("the salt %q: base64 of %d bytes expected (err=%v)", parts[2], MinSaltLen, err)
	}
	if key, err := base64.StdEncoding.DecodeString(parts[3]); err != nil || len(key) != KeyLen {
		t.Errorf("the key %q: base64 of %d bytes expected (err=%v)", parts[3], KeyLen, err)
	}

	ph, err := Parse(hash)
	if err != nil {
		t.Fatalf("Parse(%q): %v", hash, err)
	}
	if ph.Iterations != fastIterations || len(ph.Salt) != MinSaltLen || len(ph.Key) != KeyLen {
		t.Errorf("Parse: the parameters %d/%d/%d, %d/%d/%d expected",
			ph.Iterations, len(ph.Salt), len(ph.Key), fastIterations, MinSaltLen, KeyLen)
	}
	if !ph.Verify(password) {
		t.Errorf("Verify(the own password) = false, want true")
	}
	if ph.Verify("a wrong password") {
		t.Errorf("Verify(a foreign password) = true, want false")
	}
	if ph.Verify("") {
		t.Errorf("Verify(an empty password) = true, want false")
	}
}

// iterations <= 0 — DefaultIterations (600,000, OWASP; dashboard spec
// §7.2). Verification is not repeated: the roundtrip is verified in
// TestGenerateParseVerify, here — only the iterations declared at
// generation.
func TestGenerateDefaultIterations(t *testing.T) {
	hash, err := Generate("password", 0)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	ph, err := Parse(hash)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if ph.Iterations != DefaultIterations {
		t.Errorf("the default iterations = %d, want %d", ph.Iterations, DefaultIterations)
	}
}

// The salt is random: two hashes of one password differ, both verify.
func TestGenerateUniqueSalt(t *testing.T) {
	h1, err := Generate("password", fastIterations)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Generate("password", fastIterations)
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h2 {
		t.Errorf("two hashes of one password matched — the salt is not random: %q", h1)
	}
	p1, p2 := mustParse(t, h1), mustParse(t, h2)
	if bytes.Equal(p1.Salt, p2.Salt) {
		t.Errorf("the salts matched: %x", p1.Salt)
	}
	if !p1.Verify("password") || !p2.Verify("password") {
		t.Errorf("both hashes must verify against the password")
	}
}

// An empty password is not hashed: credentials are mandatory
// (dashboard spec §7.7).
func TestGenerateEmptyPassword(t *testing.T) {
	if _, err := Generate("", fastIterations); err == nil {
		t.Errorf("Generate(an empty password): an error expected")
	}
}

// Strict parsing: broken string variants are rejected with the
// component named (scheme/iterations/salt/key).
func TestParseErrors(t *testing.T) {
	salt := b64(bytes.Repeat([]byte{0xAB}, MinSaltLen))
	key := b64(bytes.Repeat([]byte{0xAB}, KeyLen))
	valid := Scheme + "$1000$" + salt + "$" + key
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"an empty string", "", "pbkdf2-sha256"},
		{"a foreign scheme", "pbkdf2-sha512$1000$" + salt + "$" + key, "pbkdf2-sha256"},
		{"no scheme", "1000$" + salt + "$" + key, "pbkdf2-sha256"},
		{"too few components", Scheme + "$1000$" + salt, "pbkdf2-sha256"},
		{"too many components", valid + "$extra", "pbkdf2-sha256"},
		{"iterations not a number", strings.Replace(valid, "$1000$", "$many$", 1), `got "many"`},
		{"zero iterations", strings.Replace(valid, "$1000$", "$0$", 1), "iterations"},
		{"negative iterations", strings.Replace(valid, "$1000$", "$-1$", 1), "iterations"},
		{"the salt is not base64", strings.Replace(valid, salt, "not-base64!", 1), "salt: base64"},
		{"a salt shorter than 16 bytes", strings.Replace(valid, salt, b64([]byte("short-salt")), 1), "salt"},
		{"the key is not base64", strings.Replace(valid, key, "??", 1), "key: base64"},
		{"a 31-byte key", strings.Replace(valid, key, b64(bytes.Repeat([]byte{1}, 31)), 1), "key"},
		{"a 33-byte key", strings.Replace(valid, key, b64(bytes.Repeat([]byte{1}, 33)), 1), "key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.in)
			if err == nil {
				t.Fatalf("Parse(%q): an error expected, got success", tt.in)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Parse(%q): the error %q does not point to %q", tt.in, err, tt.want)
			}
		})
	}
}

// Incompatible string parameters. Tampered iterations are
// syntactically correct (dashboard spec §7.2: iterations are read
// from the string) — but the derivation differs, there is no match;
// this is protection against "upgrading" the strength by editing the
// number. A salt longer than the minimum is a valid format (≥ 16
// bytes), verification works.
func TestVerifyIncompatibleParams(t *testing.T) {
	hash, err := Generate("password", fastIterations)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(hash, "$"+strconv.Itoa(fastIterations)+"$", "$"+strconv.Itoa(fastIterations+1)+"$", 1)
	ph, err := Parse(tampered)
	if err != nil {
		t.Fatalf("tampered iterations — a valid format, Parse: %v", err)
	}
	if ph.Iterations != fastIterations+1 {
		t.Errorf("the iterations from the string = %d, want %d", ph.Iterations, fastIterations+1)
	}
	if ph.Verify("password") {
		t.Errorf("a hash with foreign iterations verified against the password — the derivation must differ")
	}

	// A 24-byte salt (≥ the minimum) is an acceptable format: the
	// Derive string assembly folds back.
	long := PasswordHash{Iterations: fastIterations, Salt: bytes.Repeat([]byte{7}, 24)}
	key, err := long.Derive("password")
	if err != nil {
		t.Fatal(err)
	}
	long.Key = key
	phLong, err := Parse(Scheme + "$" + strconv.Itoa(fastIterations) + "$" + b64(long.Salt) + "$" + b64(key))
	if err != nil {
		t.Fatalf("a 24-byte salt — a valid format, Parse: %v", err)
	}
	if !phLong.Verify("password") {
		t.Errorf("a hash with a 24-byte salt must verify")
	}
}

func mustParse(t *testing.T, s string) PasswordHash {
	t.Helper()
	ph, err := Parse(s)
	if err != nil {
		t.Fatalf("Parse(%q): %v", s, err)
	}
	return ph
}

// --- parameter boundaries (dashboard spec §7.2: salt ≥ 16, iterations ≥ 1) ----

// TestGenerateSaltLen — the generation salt is exactly 16 bytes, by a
// literal: the MinSaltLen symbol would follow the constant's mutant
// and would not fix the length.
func TestGenerateSaltLen(t *testing.T) {
	hash, err := Generate("password", fastIterations)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	salt, err := base64.StdEncoding.DecodeString(strings.Split(hash, "$")[2])
	if err != nil {
		t.Fatalf("the salt %q: %v", strings.Split(hash, "$")[2], err)
	}
	if len(salt) != 16 {
		t.Errorf("the generation salt = %d bytes, want exactly 16 (dashboard spec §7.2)", len(salt))
	}
}

// TestParseSaltBounds — the minimum salt boundary at parsing: exactly
// 16 bytes is valid, 15 is a refusal with the threshold in the text.
// The 16/15 literals hold the MinSaltLen±1 mutants (the constant's
// symbol in the test would follow the mutant).
func TestParseSaltBounds(t *testing.T) {
	key := b64(bytes.Repeat([]byte{0xAB}, 32))
	exact := Scheme + "$1000$" + b64(bytes.Repeat([]byte{0xC1}, 16)) + "$" + key
	if _, err := Parse(exact); err != nil {
		t.Errorf("a salt of exactly 16 bytes: Parse: %v — the minimum is reachable (dashboard spec §7.2, ≥ 16)", err)
	}
	short := Scheme + "$1000$" + b64(bytes.Repeat([]byte{0xC1}, 15)) + "$" + key
	if _, err := Parse(short); err == nil || !strings.Contains(err.Error(), "minimum 16 bytes, got 15") {
		t.Errorf("a 15-byte salt: err = %v, want a minimum of 16 bytes, got 15", err)
	}
}

// TestGenerateOneIteration — iterations 1: already a positive integer,
// the default does not kick in (the "<= 0" boundary against "<= 1");
// verification with one iteration works.
func TestGenerateOneIteration(t *testing.T) {
	hash, err := Generate("password", 1)
	if err != nil {
		t.Fatalf("Generate(password, 1): %v", err)
	}
	ph := mustParse(t, hash)
	if ph.Iterations != 1 {
		t.Errorf("the iterations = %d, want 1 — a positive value is not replaced by the default", ph.Iterations)
	}
	if !ph.Verify("password") {
		t.Error("Verify(the own password) = false at 1 iteration, want true")
	}
}

// TestParseIterationBounds — iteration boundaries at parsing: exactly
// 1 is valid (the "< 1" → "<= 1"/"< 2" mutants); an Atoi overflow is
// a refusal (cutting "err != nil" would let through the interval
// clamped to MaxInt64); the error text names the iterations component
// (the parts[1] → parts[0] and parts[2] mutants).
func TestParseIterationBounds(t *testing.T) {
	salt := b64(bytes.Repeat([]byte{0xC1}, 16))
	key := b64(bytes.Repeat([]byte{0xAB}, 32))

	if _, err := Parse(Scheme + "$1$" + salt + "$" + key); err != nil {
		t.Errorf("the iterations of exactly 1: Parse: %v — a positive integer is valid", err)
	}
	if big, err := Parse(Scheme + "$99999999999999999999$" + salt + "$" + key); err == nil {
		t.Fatalf("an iterations overflow: Parse = %+v, want an error", big)
	} else if !strings.Contains(err.Error(), "iterations") {
		t.Errorf("the overflow: the error %q does not point to the iterations", err)
	}
	if _, err := Parse(Scheme + "$twenty$" + salt + "$" + key); err == nil ||
		!strings.Contains(err.Error(), `got "twenty"`) {
		t.Errorf("the iterations are not a number: err = %v, want a text with got \"twenty\"", err)
	}
}

// TestVerifyZeroHash — a zero PasswordHash (constructed outside Parse)
// never verifies: the degenerate derivation does not match an empty
// key (subtle compares lengths — 32 against 0).
func TestVerifyZeroHash(t *testing.T) {
	if (PasswordHash{}).Verify("password") {
		t.Error("Verify on a zero hash = true, want false")
	}
}

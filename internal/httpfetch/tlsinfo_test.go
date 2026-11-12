// TLS parsing tests (T-050, spec 5.5 item 3, §8). Two levels:
//
//   - direct newTLSInfoAt calls on handmade tls.ConnectionState values:
//     mockable clocks (expired/not_yet_valid), SAN match/mismatch
//     (VerifyHostname, including the CN fallback and wildcards),
//     self-signedness, field and chain extraction;
//   - an end-to-end Fetch through httptest TLS servers with certificates
//     issued by the tests' own mini-CA (crypto/x509.CreateCertificate):
//     a self-signed server, a leaf+CA chain, http without TLS.

package httpfetch

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"testing"
	"time"
)

// --- helpers: the tests' own mini-PKI ----------------------------------------

// tlsTestCA is the tests' homemade certificate authority: it issues leaf
// certificates without external dependencies or the network.
type tlsTestCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
}

// newTLSTestCA is a self-signed CA (CN cn, O "tlsinfo tests").
func newTLSTestCA(t *testing.T, cn string) *tlsTestCA {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("the CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"tlsinfo tests"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatalf("the CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing the CA certificate: %v", err)
	}
	return &tlsTestCA{cert: cert, key: key}
}

// issueTLSLeaf issues a leaf certificate (the SAN dnsNames/ips, the
// validity window notBefore…notAfter, O "Leaf Org", serial 0x2a). Signed
// by the parent; a nil parent means self-signed.
func issueTLSLeaf(t *testing.T, parent *x509.Certificate, parentKey crypto.Signer, cn string,
	dnsNames []string, ips []net.IP, notBefore, notAfter time.Time,
) (*x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("the leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(0x2a),
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"Leaf Org"}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
	}
	signerCert, signerKey := parent, parentKey
	if signerCert == nil {
		signerCert, signerKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signerCert, pub, signerKey)
	if err != nil {
		t.Fatalf("the leaf certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing the leaf certificate: %v", err)
	}
	return cert, key
}

// tlsChainState is a connection state with a server chain (the leaf
// first).
func tlsChainState(certs ...*x509.Certificate) *tls.ConnectionState {
	return &tls.ConnectionState{PeerCertificates: certs}
}

// tlsServerWithChain is an httptest TLS server serving the given chain
// (Certificate[0] is the leaf, then intermediaries; like a real web
// server).
func tlsServerWithChain(t *testing.T, chain tls.Certificate) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{chain}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// fetchInsecure is a Fetch with InsecureSkipVerify (spec 5.5 item 3: the
// connect is always insecurely verified; NewTLSInfo does the checking).
func fetchInsecure(t *testing.T, rawurl string) *FetchResult {
	t.Helper()
	u, err := url.Parse(rawurl)
	if err != nil {
		t.Fatalf("URL %q: %v", rawurl, err)
	}
	f := NewFetcher(Options{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch %s: %v", rawurl, err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	return res
}

// --- mockable clocks: the validity window ------------------------------------

// TestTLSInfoVerificationWindow: a certificate within the window is
// valid; before NotBefore — not_yet_valid; after NotAfter — expired. The
// clocks are mocked (newTLSInfoAt); the test's real time has no effect.
func TestTLSInfoVerificationWindow(t *testing.T) {
	ca := newTLSTestCA(t, "Window CA")
	notBefore := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := notBefore.Add(24 * time.Hour)
	leaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "window.example.net",
		[]string{"window.example.net"}, nil, notBefore, notAfter)
	cs := tlsChainState(leaf, ca.cert)

	for _, tc := range []struct {
		name string
		now  time.Time
		want TLSProblem // "" — no reasons
	}{
		{"before the window", notBefore.Add(-time.Hour), TLSProblemNotYetValid},
		{"inside the window", notBefore.Add(12 * time.Hour), ""},
		{"after the window", notAfter.Add(time.Hour), TLSProblemExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := newTLSInfoAt(cs, "window.example.net", tc.now)
			if info == nil {
				t.Fatal("TLSInfo nil")
			}
			if tc.want == "" {
				if !info.Valid || len(info.Problems) != 0 {
					t.Errorf("valid without reasons expected, got valid=%v problems=%v", info.Valid, info.Problems)
				}
				return
			}
			if info.Valid {
				t.Errorf("invalid (%s) expected, got valid", tc.want)
			}
			if len(info.Problems) != 1 || info.Problems[0] != tc.want {
				t.Errorf("reasons: got %v, [%s] expected", info.Problems, tc.want)
			}
		})
	}
}

// --- the domain in SAN/CN -----------------------------------------------------

// TestTLSInfoHostnameMatch: an SAN match (including wildcard and IP) is
// valid; a mismatch is hostname_mismatch; without SAN the name is looked
// up in the CN.
func TestTLSInfoHostnameMatch(t *testing.T) {
	ca := newTLSTestCA(t, "Hostname CA")
	now := time.Now()
	leaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "other.example.net",
		[]string{"other.example.net", "*.wild.example.net"}, nil, now.Add(-time.Hour), now.Add(time.Hour))
	cs := tlsChainState(leaf, ca.cert)

	for _, tc := range []struct {
		host     string
		wantProb TLSProblem // "" — no reasons
	}{
		{"other.example.net", ""},
		{"sub.wild.example.net", ""},
		{"match.example.net", TLSProblemHostnameMismatch},
	} {
		info := newTLSInfoAt(cs, tc.host, now)
		if info == nil {
			t.Fatalf("host %s: TLSInfo nil", tc.host)
		}
		if tc.wantProb == "" && !info.Valid {
			t.Errorf("host %s: valid expected, got %v", tc.host, info.Problems)
		}
		if tc.wantProb != "" && (info.Valid || len(info.Problems) != 1 || info.Problems[0] != tc.wantProb) {
			t.Errorf("host %s: the reason %s expected, got valid=%v %v", tc.host, tc.wantProb, info.Valid, info.Problems)
		}
	}

	// A SAN miss and the CN fallback: with no SAN at all the name is taken
	// from the CN.
	cnLeaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "legacy.example.net",
		nil, nil, now.Add(-time.Hour), now.Add(time.Hour))
	for host, wantValid := range map[string]bool{
		"legacy.example.net": true,
		"else.example.net":   false,
	} {
		info := newTLSInfoAt(tlsChainState(cnLeaf, ca.cert), host, now)
		if info.Valid != wantValid {
			t.Errorf("CN-fallback %s: valid=%v, %v expected (problems %v)", host, info.Valid, wantValid, info.Problems)
		}
	}

	// An IP host (connecting to an IP literal): the SAN iPAddress.
	ipLeaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "ip.example.net",
		nil, []net.IP{net.ParseIP("127.0.0.1")}, now.Add(-time.Hour), now.Add(time.Hour))
	for host, wantValid := range map[string]bool{
		"127.0.0.1": true,
		"127.0.0.2": false,
	} {
		info := newTLSInfoAt(tlsChainState(ipLeaf, ca.cert), host, now)
		if info.Valid != wantValid {
			t.Errorf("an IP host %s: valid=%v, %v expected (problems %v)", host, info.Valid, wantValid, info.Problems)
		}
	}
}

// --- self-signedness ---------------------------------------------------------

// TestTLSInfoSelfSigned: a leaf signed by its own key is invalid with the
// self_signed reason (standard verification does not let such a
// certificate through) and the SelfSigned field is set.
func TestTLSInfoSelfSigned(t *testing.T) {
	now := time.Now()
	leaf, _ := issueTLSLeaf(t, nil, nil, "self.example.net",
		[]string{"self.example.net"}, nil, now.Add(-time.Hour), now.Add(time.Hour))
	info := newTLSInfoAt(tlsChainState(leaf), "self.example.net", now)
	if info == nil {
		t.Fatal("TLSInfo nil")
	}
	if info.Valid {
		t.Error("a self-signed certificate recognized as valid")
	}
	if !info.SelfSigned {
		t.Error("SelfSigned is not set")
	}
	if len(info.Problems) != 1 || info.Problems[0] != TLSProblemSelfSigned {
		t.Errorf("reasons: got %v, [self_signed] expected", info.Problems)
	}
	if info.ChainLength != 1 || len(info.ChainSubjects) != 1 {
		t.Errorf("the self-signed chain: length %d, subjects %v", info.ChainLength, info.ChainSubjects)
	}
}

// --- field extraction ---------------------------------------------------------

// TestTLSInfoFields: issuer/subject (CN, O), the serial number (hex), the
// validity window, the SAN (DNS and IP), the chain (length, subjects), and
// the JSON tags for the subdomain_results.tls column (spec 6.1).
func TestTLSInfoFields(t *testing.T) {
	ca := newTLSTestCA(t, "Fields CA")
	notBefore := time.Date(2031, 6, 1, 0, 0, 0, 0, time.UTC)
	notAfter := notBefore.Add(90 * 24 * time.Hour)
	leaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "fields.example.net",
		[]string{"fields.example.net"},
		[]net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("2001:db8::1")},
		notBefore, notAfter)
	info := newTLSInfoAt(tlsChainState(leaf, ca.cert), "fields.example.net", notBefore.Add(time.Hour))
	if info == nil {
		t.Fatal("TLSInfo nil")
	}

	if info.SubjectCommonName != "fields.example.net" || info.SubjectOrganization != "Leaf Org" {
		t.Errorf("the subject: CN=%q O=%q", info.SubjectCommonName, info.SubjectOrganization)
	}
	if info.IssuerCommonName != "Fields CA" || info.IssuerOrganization != "tlsinfo tests" {
		t.Errorf("the issuer: CN=%q O=%q", info.IssuerCommonName, info.IssuerOrganization)
	}
	if info.SerialNumber != "2a" {
		t.Errorf("the serial number: %q, 2a (hex) expected", info.SerialNumber)
	}
	if !info.NotBefore.Equal(notBefore) || !info.NotAfter.Equal(notAfter) {
		t.Errorf("the validity: %v…%v, %v…%v expected", info.NotBefore, info.NotAfter, notBefore, notAfter)
	}
	if len(info.DNSNames) != 1 || info.DNSNames[0] != "fields.example.net" {
		t.Errorf("SAN DNS: %v", info.DNSNames)
	}
	wantIPs := []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("2001:db8::1")}
	if len(info.IPAddresses) != 2 || info.IPAddresses[0] != wantIPs[0] || info.IPAddresses[1] != wantIPs[1] {
		t.Errorf("SAN IPs: %v, %v expected", info.IPAddresses, wantIPs)
	}
	if info.ChainLength != 2 {
		t.Errorf("the chain length: %d, 2 expected", info.ChainLength)
	}
	wantSubjects := []string{leaf.Subject.String(), ca.cert.Subject.String()}
	if len(info.ChainSubjects) != 2 || info.ChainSubjects[0] != wantSubjects[0] || info.ChainSubjects[1] != wantSubjects[1] {
		t.Errorf("the chain subjects: %v, %v expected", info.ChainSubjects, wantSubjects)
	}
	if !info.Valid || info.SelfSigned {
		t.Errorf("a CA-signed certificate inside the window: valid=%v self_signed=%v, true/false expected", info.Valid, info.SelfSigned)
	}

	// JSON tags: the subdomain_results.tls column (jsonb) — the
	// serialization is stable, empty problems are omitted (omitempty).
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("parsing the json: %v", err)
	}
	for _, key := range []string{"valid", "not_before", "not_after", "chain_length", "serial"} {
		if _, ok := m[key]; !ok {
			t.Errorf("the json without the key %q: %s", key, b)
		}
	}
	if _, ok := m["problems"]; ok {
		t.Errorf("empty problems are not omitted: %s", b)
	}
}

// TestTLSInfoNoTLS: a connection without TLS (http) or without server
// certificates is nil, not an error (§8).
func TestTLSInfoNoTLS(t *testing.T) {
	if got := NewTLSInfo(nil, "x.example.net"); got != nil {
		t.Errorf("a nil state: got %v", got)
	}
	if got := NewTLSInfo(&tls.ConnectionState{}, "x.example.net"); got != nil {
		t.Errorf("without PeerCertificates: got %v", got)
	}
}

// --- end-to-end Fetch -----------------------------------------------------------

// TestFetchTLSInfoSelfSigned: a self-signed server — the connection is
// established (InsecureSkipVerify), the TLSInfo is assembled and marks the
// self_signed reason.
func TestFetchTLSInfoSelfSigned(t *testing.T) {
	now := time.Now()
	leaf, key := issueTLSLeaf(t, nil, nil, "self.example.net",
		[]string{"self.example.net"}, nil, now.Add(-time.Hour), now.Add(time.Hour))
	srv := tlsServerWithChain(t, tls.Certificate{
		Certificate: [][]byte{leaf.Raw},
		PrivateKey:  key,
	})

	res := fetchInsecure(t, hostURL(srv.URL, "self.example.net").String())
	if res.TLS == nil || res.TLSInfo == nil {
		t.Fatalf("the TLS connection is not established: TLS=%v TLSInfo=%v", res.TLS, res.TLSInfo)
	}
	info := res.TLSInfo
	if info.Valid || len(info.Problems) != 1 || info.Problems[0] != TLSProblemSelfSigned {
		t.Errorf("a self-signed server: valid=%v problems=%v", info.Valid, info.Problems)
	}
	if !info.SelfSigned || info.ChainLength != 1 {
		t.Errorf("SelfSigned=%v ChainLength=%d", info.SelfSigned, info.ChainLength)
	}
	if info.SubjectCommonName != "self.example.net" {
		t.Errorf("the subject CN: %q", info.SubjectCommonName)
	}
}

// TestFetchTLSInfoChainAndHostname: a server with a leaf+CA chain — the
// Fetch fills TLSInfo by the URL's host (SNI/Host): a match is valid, the
// chain is visible in full; a mismatch is hostname_mismatch.
func TestFetchTLSInfoChainAndHostname(t *testing.T) {
	ca := newTLSTestCA(t, "Chain CA")
	now := time.Now()
	leaf, key := issueTLSLeaf(t, ca.cert, ca.key, "match.example.net",
		[]string{"match.example.net"}, nil, now.Add(-time.Hour), now.Add(time.Hour))
	srv := tlsServerWithChain(t, tls.Certificate{
		Certificate: [][]byte{leaf.Raw, ca.cert.Raw},
		PrivateKey:  key,
	})

	res := fetchInsecure(t, hostURL(srv.URL, "match.example.net").String())
	if res.TLSInfo == nil {
		t.Fatal("TLSInfo nil")
	}
	info := res.TLSInfo
	if !info.Valid {
		t.Errorf("a SAN match: valid=%v problems=%v", info.Valid, info.Problems)
	}
	if info.SelfSigned {
		t.Error("a CA-signed leaf is marked self-signed")
	}
	if info.IssuerCommonName != "Chain CA" {
		t.Errorf("the issuer: %q, Chain CA expected", info.IssuerCommonName)
	}
	if info.ChainLength != 2 {
		t.Errorf("the chain length: %d, 2 expected (leaf+CA)", info.ChainLength)
	}
	wantSubjects := []string{leaf.Subject.String(), ca.cert.Subject.String()}
	if len(info.ChainSubjects) != 2 || info.ChainSubjects[0] != wantSubjects[0] || info.ChainSubjects[1] != wantSubjects[1] {
		t.Errorf("the chain subjects: %v, %v expected", info.ChainSubjects, wantSubjects)
	}

	res = fetchInsecure(t, hostURL(srv.URL, "wrong.example.net").String())
	if res.TLSInfo == nil {
		t.Fatal("TLSInfo nil on a mismatch")
	}
	if res.TLSInfo.Valid || len(res.TLSInfo.Problems) != 1 ||
		res.TLSInfo.Problems[0] != TLSProblemHostnameMismatch {
		t.Errorf("a SAN mismatch: valid=%v problems=%v", res.TLSInfo.Valid, res.TLSInfo.Problems)
	}
}

// TestFetchTLSInfoPlainHTTP: an http response has no TLS and no TLSInfo.
func TestFetchTLSInfoPlainHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "plain")
	}))
	t.Cleanup(srv.Close)

	res := fetchInsecure(t, srv.URL)
	if res.TLS != nil || res.TLSInfo != nil {
		t.Errorf("http without TLS: TLS=%v TLSInfo=%v, nil/nil expected", res.TLS, res.TLSInfo)
	}
}

// --- error classification (§8) --------------------------------------------------

// tlsTestNetError is a network error (a timeout) for classification.
type tlsTestNetError struct{}

func (tlsTestNetError) Error() string   { return "i/o timeout" }
func (tlsTestNetError) Timeout() bool   { return true }
func (tlsTestNetError) Temporary() bool { return true }

// TestIsTLSCertError: certificate/TLS protocol errors are distinguishable
// from network ones (timeouts, cancellation) and others.
func TestIsTLSCertError(t *testing.T) {
	cert := &x509.Certificate{}
	verifyErr := &tls.CertificateVerificationError{Err: errors.New("verification failed")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"verification", verifyErr, true},
		{"a wrapped verification", fmt.Errorf("httpfetch: GET https://x: %w", verifyErr), true},
		{"hostname", x509.HostnameError{Certificate: cert, Host: "h"}, true},
		{"expired", x509.CertificateInvalidError{Cert: cert, Reason: x509.Expired}, true},
		{"an unknown issuer", x509.UnknownAuthorityError{Cert: cert}, true},
		{"no system roots", x509.SystemRootsError{}, true},
		{"a non-TLS response", tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}, true},
		{"refused by an alert", tls.AlertError(40), true},
		{"a network timeout", tlsTestNetError{}, false},
		{"a context cancellation", context.DeadlineExceeded, false},
		{"other", errors.New("an ordinary error"), false},
	}
	for _, tc := range cases {
		if got := IsTLSCertError(tc.err); got != tc.want {
			t.Errorf("%s: IsTLSCertError=%v, %v expected", tc.name, got, tc.want)
		}
	}
}

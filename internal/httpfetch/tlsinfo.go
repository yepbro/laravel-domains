package httpfetch

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/netip"
	"strings"
	"time"
)

// TLSProblem is a typed reason why the leaf certificate failed its own
// verification (spec 5.5 item 3). The codes are stable for storing in
// subdomain_results.tls (jsonb, spec 6.1) — the idn.ErrorKind style: a
// string type with no parsing back.
type TLSProblem string

const (
	// TLSProblemExpired — the certificate's validity period has ended
	// (NotAfter earlier than the check moment).
	TLSProblemExpired TLSProblem = "expired"
	// TLSProblemNotYetValid — the certificate is not valid yet (NotBefore
	// later than the check moment).
	TLSProblemNotYetValid TLSProblem = "not_yet_valid"
	// TLSProblemHostnameMismatch — the connection host (SNI/Host) was not
	// found in either SAN or CN (x509.HostnameError; see
	// verifyCertHostname).
	TLSProblemHostnameMismatch TLSProblem = "hostname_mismatch"
	// TLSProblemSelfSigned — the leaf is signed by its own key (a
	// CheckSignature over its own signature): standard verification does
	// not let such a certificate through — an unknown issuer.
	TLSProblemSelfSigned TLSProblem = "self_signed"
)

// String returns the reason code as a string ("expired", …).
func (p TLSProblem) String() string { return string(p) }

// TLSInfo is the parsing of the established TLS connection's certificate
// for storing in the check result (spec 5.5 item 3: InsecureSkipVerify =
// true for connecting — the content is needed — but the certificate check
// result, issuer, validity window and SAN are stored; spec 5.5 item 7,
// the subdomain_results.tls jsonb column — spec 6.1). It is assembled by
// NewTLSInfo from tls.ConnectionState after the handshake; for a
// connection without TLS — nil, which is not an error (§8).
//
// The mechanics of the own verification: the connection is established
// with InsecureSkipVerify = true, and the transport performs the check
// itself on the leaf certificate (PeerCertificates[0] — with several
// server certificates the first one counts as the leaf; mTLS scenarios are
// no different): the connection host in SAN/CN (VerifyHostname — SAN, or
// CN when they are absent), the NotBefore/NotAfter validity window, self-
// signedness. The trust chain to the system roots is not verified: the
// result must not depend on the container's root store, and a bad
// certificate does not block the fetch (§8) — chain attributes are stored
// informationally (ChainLength/ChainSubjects).
type TLSInfo struct {
	// Valid — the leaf passed all checks (Problems is empty). The
	// certificate's "days left" is computed by the consumer from NotAfter —
	// storing values derived from time makes no sense.
	Valid bool `json:"valid"`
	// Problems — the reasons the check failed; empty when Valid.
	Problems []TLSProblem `json:"problems,omitempty"`

	// The leaf certificate.

	// SubjectCommonName, SubjectOrganization — the subject (CN, O).
	SubjectCommonName   string `json:"subject_cn,omitempty"`
	SubjectOrganization string `json:"subject_o,omitempty"`
	// IssuerCommonName, IssuerOrganization — the issuer (CN, O).
	IssuerCommonName   string `json:"issuer_cn,omitempty"`
	IssuerOrganization string `json:"issuer_o,omitempty"`
	// SerialNumber is the serial number, hex without leading zeros.
	SerialNumber string `json:"serial,omitempty"`
	// NotBefore, NotAfter — the certificate's validity window.
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after"`
	// SelfSigned — the leaf is signed by its own key.
	SelfSigned bool `json:"self_signed"`

	// SAN is the leaf's subjectAltName: DNS names and IP addresses.
	DNSNames    []string     `json:"dns_names,omitempty"`
	IPAddresses []netip.Addr `json:"ip_addresses,omitempty"`

	// The server's chain in transmission order (the leaf first): the
	// length and the certificates' subjects in RFC 2253.
	ChainLength   int      `json:"chain_length"`
	ChainSubjects []string `json:"chain_subjects,omitempty"`
}

// NewTLSInfo assembles TLSInfo from the established connection's state and
// performs its own verification of the leaf certificate against host —
// the request URL's host (SNI/Host; a domain or an IP literal, no port).
// cs == nil or without server certificates (http, anonymous ciphers) —
// nil: the absence of TLS is not an error (§8). Callers of RoundTrip
// directly (an http.Client over the Fetcher, the T-048 redirect chains)
// call NewTLSInfo themselves on resp.TLS and the final URL's host.
func NewTLSInfo(cs *tls.ConnectionState, host string) *TLSInfo {
	return newTLSInfoAt(cs, host, time.Now())
}

// newTLSInfoAt is NewTLSInfo with mockable clocks: deterministic
// expired/not_yet_valid checks in unit tests.
func newTLSInfoAt(cs *tls.ConnectionState, host string, now time.Time) *TLSInfo {
	if cs == nil || len(cs.PeerCertificates) == 0 {
		return nil
	}
	leaf := cs.PeerCertificates[0]

	info := &TLSInfo{
		SubjectCommonName:   leaf.Subject.CommonName,
		SubjectOrganization: firstString(leaf.Subject.Organization),
		IssuerCommonName:    leaf.Issuer.CommonName,
		IssuerOrganization:  firstString(leaf.Issuer.Organization),
		NotBefore:           leaf.NotBefore,
		NotAfter:            leaf.NotAfter,
		DNSNames:            append([]string(nil), leaf.DNSNames...),
		ChainLength:         len(cs.PeerCertificates),
		ChainSubjects:       make([]string, 0, len(cs.PeerCertificates)),
	}
	if leaf.SerialNumber != nil {
		info.SerialNumber = leaf.SerialNumber.Text(16)
	}
	for _, ip := range leaf.IPAddresses {
		if addr, ok := netip.AddrFromSlice(ip); ok {
			info.IPAddresses = append(info.IPAddresses, addr.Unmap())
		}
	}
	for _, cert := range cs.PeerCertificates {
		info.ChainSubjects = append(info.ChainSubjects, cert.Subject.String())
	}

	// Own verification (spec 5.5 item 3): the reason order is the validity
	// window, the domain, self-signedness; the check keeps collecting
	// reasons after the first one (the consumer sees everything).
	if now.Before(leaf.NotBefore) {
		info.addProblem(TLSProblemNotYetValid)
	}
	if now.After(leaf.NotAfter) {
		info.addProblem(TLSProblemExpired)
	}
	if host != "" {
		if err := verifyCertHostname(leaf, host); err != nil {
			info.addProblem(TLSProblemHostnameMismatch)
		}
	}
	// The leaf's signature verifies with its own key — self-signedness
	// without requiring IsCA (CheckSignatureFrom rejects a non-CA parent
	// with ConstraintViolationError even before the signature check).
	if err := leaf.CheckSignature(leaf.SignatureAlgorithm,
		leaf.RawTBSCertificate, leaf.Signature); err == nil {
		info.SelfSigned = true
		info.addProblem(TLSProblemSelfSigned)
	}
	info.Valid = len(info.Problems) == 0
	return info
}

// addProblem adds a check failure reason code.
func (i *TLSInfo) addProblem(p TLSProblem) {
	if i.Problems == nil {
		i.Problems = make([]TLSProblem, 0, 1)
	}
	i.Problems = append(i.Problems, p)
}

// verifyCertHostname is the host's domain in the certificate's SAN/CN.
// The basis is the standard VerifyHostname (SAN, wildcard, IP literals).
// Spec 5.5 item 3 requires SAN/CN, while modern crypto/x509 ignores the CN
// even when SAN is absent, so for certificates without dNSName SANs the
// name is checked against the CommonName (RFC 6125 §6.4.4, the pre-SAN-era
// behavior).
func verifyCertHostname(cert *x509.Certificate, host string) error {
	err := cert.VerifyHostname(host)
	if err == nil {
		return nil
	}
	var hostErr x509.HostnameError
	if len(cert.DNSNames) == 0 && errors.As(err, &hostErr) &&
		strings.EqualFold(host, cert.Subject.CommonName) {
		return nil
	}
	return err
}

// firstString is the first slice element or "" (pkix.Organization is a
// slice).
func firstString(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// IsTLSCertError classifies a TLS layer error (§8): whether it was caused
// by a bad certificate or a TLS protocol refusal, not by the network.
// Certificate ones: x509 verification errors (relevant when the caller did
// not set InsecureSkipVerify), a server refusal with an alert (AlertError),
// a non-TLS response on a TLS port (RecordHeaderError), and the absence of
// system roots. Network ones (timeouts, connection drops, connection
// refusals) belong to IsTimeout and the consumer; they are not "a bad
// certificate" and do not block storing the data of the connection that
// had been established before the error.
func IsTLSCertError(err error) bool {
	if err == nil {
		return false
	}
	var (
		verifyErr     *tls.CertificateVerificationError
		hostnameErr   x509.HostnameError
		invalidErr    x509.CertificateInvalidError
		authorityErr  x509.UnknownAuthorityError
		constraintErr x509.ConstraintViolationError
		rootsErr      x509.SystemRootsError
		recordErr     tls.RecordHeaderError
		alertErr      tls.AlertError
	)
	switch {
	case errors.As(err, &verifyErr),
		errors.As(err, &hostnameErr),
		errors.As(err, &invalidErr),
		errors.As(err, &authorityErr),
		errors.As(err, &constraintErr),
		errors.As(err, &rootsErr),
		errors.As(err, &recordErr),
		errors.As(err, &alertErr):
		return true
	}
	return false
}

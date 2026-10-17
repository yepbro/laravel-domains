// Тесты TLS-разбора (T-050, ТЗ 5.5 п. 3, §8). Два уровня:
//
//   - прямые вызовы newTLSInfoAt на рукотворных tls.ConnectionState:
//     заглушаемые часы (expired/not_yet_valid), SAN-матч/мисматч
//     (VerifyHostname, включая CN-fallback и wildcard), самоподписанность,
//     извлечение полей и цепочки;
//   - сквозной Fetch через httptest TLS-серверы с сертификатами,
//     выпущенными собственным мини-CA (crypto/x509.CreateCertificate):
//     самоподписанный сервер, цепочка лист+CA, http без TLS.

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

// --- вспомогательные: собственный мини-PKI -----------------------------------

// tlsTestCA — самодельный удостоверяющий центр тестов: выпускает
// листовые сертификаты без внешних зависимостей и сети.
type tlsTestCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
}

// newTLSTestCA — самоподписанный CA (CN cn, O "tlsinfo tests").
func newTLSTestCA(t *testing.T, cn string) *tlsTestCA {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ключ CA: %v", err)
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
		t.Fatalf("сертификат CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("разбор сертификата CA: %v", err)
	}
	return &tlsTestCA{cert: cert, key: key}
}

// issueTLSLeaf выпускает листовой сертификат (SAN dnsNames/ips, окно
// действия notBefore…notAfter, O "Leaf Org", серийный 0x2a). Подпись —
// родителем; parent nil — самоподписанный.
func issueTLSLeaf(t *testing.T, parent *x509.Certificate, parentKey crypto.Signer, cn string,
	dnsNames []string, ips []net.IP, notBefore, notAfter time.Time,
) (*x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ключ листа: %v", err)
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
		t.Fatalf("сертификат листа: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("разбор сертификата листа: %v", err)
	}
	return cert, key
}

// tlsChainState — состояние соединения с цепочкой сервера (лист первый).
func tlsChainState(certs ...*x509.Certificate) *tls.ConnectionState {
	return &tls.ConnectionState{PeerCertificates: certs}
}

// tlsServerWithChain — httptest TLS-сервер, отдающий заданную цепочку
// (Certificate[0] — лист, далее посредники; как реальный веб-сервер).
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

// fetchInsecure — Fetch с InsecureSkipVerify (ТЗ 5.5 п. 3: коннект
// всегда незащищённо-верифицируемый, проверку делает NewTLSInfo).
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

// --- заглушаемые часы: окно действия ------------------------------------------

// TestTLSInfoVerificationWindow: сертификат в окне — valid; до NotBefore
// — not_yet_valid; после NotAfter — expired. Часы подменяются
// (newTLSInfoAt), реальное время теста не влияет.
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
		want TLSProblem // "" — без причин
	}{
		{"до окна", notBefore.Add(-time.Hour), TLSProblemNotYetValid},
		{"внутри окна", notBefore.Add(12 * time.Hour), ""},
		{"после окна", notAfter.Add(time.Hour), TLSProblemExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := newTLSInfoAt(cs, "window.example.net", tc.now)
			if info == nil {
				t.Fatal("TLSInfo nil")
			}
			if tc.want == "" {
				if !info.Valid || len(info.Problems) != 0 {
					t.Errorf("ожидался valid без причин, получено valid=%v problems=%v", info.Valid, info.Problems)
				}
				return
			}
			if info.Valid {
				t.Errorf("ожидался invalid (%s), получено valid", tc.want)
			}
			if len(info.Problems) != 1 || info.Problems[0] != tc.want {
				t.Errorf("причины: получено %v, ожидалась [%s]", info.Problems, tc.want)
			}
		})
	}
}

// --- домен в SAN/CN -----------------------------------------------------------

// TestTLSInfoHostnameMatch: SAN-матч (включая wildcard и IP) — valid;
// мисматч — hostname_mismatch; без SAN имя ищется в CN.
func TestTLSInfoHostnameMatch(t *testing.T) {
	ca := newTLSTestCA(t, "Hostname CA")
	now := time.Now()
	leaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "other.example.net",
		[]string{"other.example.net", "*.wild.example.net"}, nil, now.Add(-time.Hour), now.Add(time.Hour))
	cs := tlsChainState(leaf, ca.cert)

	for _, tc := range []struct {
		host     string
		wantProb TLSProblem // "" — без причин
	}{
		{"other.example.net", ""},
		{"sub.wild.example.net", ""},
		{"match.example.net", TLSProblemHostnameMismatch},
	} {
		info := newTLSInfoAt(cs, tc.host, now)
		if info == nil {
			t.Fatalf("хост %s: TLSInfo nil", tc.host)
		}
		if tc.wantProb == "" && !info.Valid {
			t.Errorf("хост %s: ожидался valid, получено %v", tc.host, info.Problems)
		}
		if tc.wantProb != "" && (info.Valid || len(info.Problems) != 1 || info.Problems[0] != tc.wantProb) {
			t.Errorf("хост %s: ожидалась причина %s, получено valid=%v %v", tc.host, tc.wantProb, info.Valid, info.Problems)
		}
	}

	// SAN-миссия и CN-fallback: без SAN совсем имя берётся из CN.
	cnLeaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "legacy.example.net",
		nil, nil, now.Add(-time.Hour), now.Add(time.Hour))
	for host, wantValid := range map[string]bool{
		"legacy.example.net": true,
		"else.example.net":   false,
	} {
		info := newTLSInfoAt(tlsChainState(cnLeaf, ca.cert), host, now)
		if info.Valid != wantValid {
			t.Errorf("CN-fallback %s: valid=%v, ожидалось %v (problems %v)", host, info.Valid, wantValid, info.Problems)
		}
	}

	// IP-хост (подключение к IP-литералу): SAN iPAddress.
	ipLeaf, _ := issueTLSLeaf(t, ca.cert, ca.key, "ip.example.net",
		nil, []net.IP{net.ParseIP("127.0.0.1")}, now.Add(-time.Hour), now.Add(time.Hour))
	for host, wantValid := range map[string]bool{
		"127.0.0.1": true,
		"127.0.0.2": false,
	} {
		info := newTLSInfoAt(tlsChainState(ipLeaf, ca.cert), host, now)
		if info.Valid != wantValid {
			t.Errorf("IP-хост %s: valid=%v, ожидалось %v (problems %v)", host, info.Valid, wantValid, info.Problems)
		}
	}
}

// --- самоподписанность --------------------------------------------------------

// TestTLSInfoSelfSigned: лист подписан собственным ключом — invalid с
// причиной self_signed (стандартная верификация такой сертификат не
// пропускает), поле SelfSigned выставлено.
func TestTLSInfoSelfSigned(t *testing.T) {
	now := time.Now()
	leaf, _ := issueTLSLeaf(t, nil, nil, "self.example.net",
		[]string{"self.example.net"}, nil, now.Add(-time.Hour), now.Add(time.Hour))
	info := newTLSInfoAt(tlsChainState(leaf), "self.example.net", now)
	if info == nil {
		t.Fatal("TLSInfo nil")
	}
	if info.Valid {
		t.Error("самоподписанный сертификат признан valid")
	}
	if !info.SelfSigned {
		t.Error("SelfSigned не выставлен")
	}
	if len(info.Problems) != 1 || info.Problems[0] != TLSProblemSelfSigned {
		t.Errorf("причины: получено %v, ожидалась [self_signed]", info.Problems)
	}
	if info.ChainLength != 1 || len(info.ChainSubjects) != 1 {
		t.Errorf("цепочка самоподписанного: длина %d, субъекты %v", info.ChainLength, info.ChainSubjects)
	}
}

// --- извлечение полей ---------------------------------------------------------

// TestTLSInfoFields: издатель/субъект (CN, O), серийный номер (hex),
// окно действия, SAN (DNS и IP), цепочка (длина, субъекты), JSON-теги
// для колонки subdomain_results.tls (ТЗ 6.1).
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
		t.Errorf("субъект: CN=%q O=%q", info.SubjectCommonName, info.SubjectOrganization)
	}
	if info.IssuerCommonName != "Fields CA" || info.IssuerOrganization != "tlsinfo tests" {
		t.Errorf("издатель: CN=%q O=%q", info.IssuerCommonName, info.IssuerOrganization)
	}
	if info.SerialNumber != "2a" {
		t.Errorf("серийный номер: %q, ожидался 2a (hex)", info.SerialNumber)
	}
	if !info.NotBefore.Equal(notBefore) || !info.NotAfter.Equal(notAfter) {
		t.Errorf("срок действия: %v…%v, ожидался %v…%v", info.NotBefore, info.NotAfter, notBefore, notAfter)
	}
	if len(info.DNSNames) != 1 || info.DNSNames[0] != "fields.example.net" {
		t.Errorf("SAN DNS: %v", info.DNSNames)
	}
	wantIPs := []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("2001:db8::1")}
	if len(info.IPAddresses) != 2 || info.IPAddresses[0] != wantIPs[0] || info.IPAddresses[1] != wantIPs[1] {
		t.Errorf("SAN IP: %v, ожидался %v", info.IPAddresses, wantIPs)
	}
	if info.ChainLength != 2 {
		t.Errorf("длина цепочки: %d, ожидалась 2", info.ChainLength)
	}
	wantSubjects := []string{leaf.Subject.String(), ca.cert.Subject.String()}
	if len(info.ChainSubjects) != 2 || info.ChainSubjects[0] != wantSubjects[0] || info.ChainSubjects[1] != wantSubjects[1] {
		t.Errorf("субъекты цепочки: %v, ожидался %v", info.ChainSubjects, wantSubjects)
	}
	if !info.Valid || info.SelfSigned {
		t.Errorf("CA-подписанный сертификат в окне: valid=%v self_signed=%v, ожидалось true/false", info.Valid, info.SelfSigned)
	}

	// JSON-теги: колонка subdomain_results.tls (jsonb) — сериализация
	// стабильна, пустые проблемы опускаются (omitempty).
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("разбор json: %v", err)
	}
	for _, key := range []string{"valid", "not_before", "not_after", "chain_length", "serial"} {
		if _, ok := m[key]; !ok {
			t.Errorf("json без ключа %q: %s", key, b)
		}
	}
	if _, ok := m["problems"]; ok {
		t.Errorf("пустые problems не опущены: %s", b)
	}
}

// TestTLSInfoNoTLS: соединение без TLS (http) или без сертификатов
// сервера — nil, не ошибка (§8).
func TestTLSInfoNoTLS(t *testing.T) {
	if got := NewTLSInfo(nil, "x.example.net"); got != nil {
		t.Errorf("nil-состояние: получено %v", got)
	}
	if got := NewTLSInfo(&tls.ConnectionState{}, "x.example.net"); got != nil {
		t.Errorf("без PeerCertificates: получено %v", got)
	}
}

// --- сквозной Fetch ------------------------------------------------------------

// TestFetchTLSInfoSelfSigned: самоподписанный сервер — соединение
// установлено (InsecureSkipVerify), TLSInfo собран и помечает причину
// self_signed.
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
		t.Fatalf("TLS-соединение не установлено: TLS=%v TLSInfo=%v", res.TLS, res.TLSInfo)
	}
	info := res.TLSInfo
	if info.Valid || len(info.Problems) != 1 || info.Problems[0] != TLSProblemSelfSigned {
		t.Errorf("самоподписанный сервер: valid=%v problems=%v", info.Valid, info.Problems)
	}
	if !info.SelfSigned || info.ChainLength != 1 {
		t.Errorf("SelfSigned=%v ChainLength=%d", info.SelfSigned, info.ChainLength)
	}
	if info.SubjectCommonName != "self.example.net" {
		t.Errorf("CN субъекта: %q", info.SubjectCommonName)
	}
}

// TestFetchTLSInfoChainAndHostname: сервер с цепочкой лист+CA —
// Fetch заполняет TLSInfo по хосту URL (SNI/Host): матч — valid,
// цепочка видна целиком; мисматч — hostname_mismatch.
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
		t.Errorf("SAN-матч: valid=%v problems=%v", info.Valid, info.Problems)
	}
	if info.SelfSigned {
		t.Error("CA-подписанный лист помечен самоподписанным")
	}
	if info.IssuerCommonName != "Chain CA" {
		t.Errorf("издатель: %q, ожидался Chain CA", info.IssuerCommonName)
	}
	if info.ChainLength != 2 {
		t.Errorf("длина цепочки: %d, ожидалась 2 (лист+CA)", info.ChainLength)
	}
	wantSubjects := []string{leaf.Subject.String(), ca.cert.Subject.String()}
	if len(info.ChainSubjects) != 2 || info.ChainSubjects[0] != wantSubjects[0] || info.ChainSubjects[1] != wantSubjects[1] {
		t.Errorf("субъекты цепочки: %v, ожидался %v", info.ChainSubjects, wantSubjects)
	}

	res = fetchInsecure(t, hostURL(srv.URL, "wrong.example.net").String())
	if res.TLSInfo == nil {
		t.Fatal("TLSInfo nil при мисматче")
	}
	if res.TLSInfo.Valid || len(res.TLSInfo.Problems) != 1 ||
		res.TLSInfo.Problems[0] != TLSProblemHostnameMismatch {
		t.Errorf("SAN-мисс: valid=%v problems=%v", res.TLSInfo.Valid, res.TLSInfo.Problems)
	}
}

// TestFetchTLSInfoPlainHTTP: http-ответ — TLS и TLSInfo отсутствуют.
func TestFetchTLSInfoPlainHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "plain")
	}))
	t.Cleanup(srv.Close)

	res := fetchInsecure(t, srv.URL)
	if res.TLS != nil || res.TLSInfo != nil {
		t.Errorf("http без TLS: TLS=%v TLSInfo=%v, ожидалось nil/nil", res.TLS, res.TLSInfo)
	}
}

// --- классификация ошибок (§8) --------------------------------------------------

// tlsTestNetError — сетевая ошибка (таймаут) для классификации.
type tlsTestNetError struct{}

func (tlsTestNetError) Error() string   { return "i/o timeout" }
func (tlsTestNetError) Timeout() bool   { return true }
func (tlsTestNetError) Temporary() bool { return true }

// TestIsTLSCertError: ошибки сертификата/протокола TLS отличимы от
// сетевых (таймауты, отмена) и прочих.
func TestIsTLSCertError(t *testing.T) {
	cert := &x509.Certificate{}
	verifyErr := &tls.CertificateVerificationError{Err: errors.New("проверка не прошла")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"верификация", verifyErr, true},
		{"обёрнутая верификация", fmt.Errorf("httpfetch: GET https://x: %w", verifyErr), true},
		{"hostname", x509.HostnameError{Certificate: cert, Host: "h"}, true},
		{"истёк", x509.CertificateInvalidError{Cert: cert, Reason: x509.Expired}, true},
		{"неизвестный издатель", x509.UnknownAuthorityError{Cert: cert}, true},
		{"нет системных корней", x509.SystemRootsError{}, true},
		{"не-TLS ответ", tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}, true},
		{"отказ предупреждением", tls.AlertError(40), true},
		{"сетевой таймаут", tlsTestNetError{}, false},
		{"отмена контекста", context.DeadlineExceeded, false},
		{"прочее", errors.New("обычная ошибка"), false},
	}
	for _, tc := range cases {
		if got := IsTLSCertError(tc.err); got != tc.want {
			t.Errorf("%s: IsTLSCertError=%v, ожидалось %v", tc.name, got, tc.want)
		}
	}
}

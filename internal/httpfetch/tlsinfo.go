package httpfetch

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/netip"
	"strings"
	"time"
)

// TLSProblem — типизированная причина, по которой листовой сертификат
// не прошёл самостоятельную проверку (ТЗ 5.5 п. 3). Коды устойчивы для
// сохранения в subdomain_results.tls (jsonb, ТЗ 6.1) — стиль
// idn.ErrorKind: строковый тип без парсинга обратно.
type TLSProblem string

const (
	// TLSProblemExpired — срок действия сертификата истёк (NotAfter
	// раньше момента проверки).
	TLSProblemExpired TLSProblem = "expired"
	// TLSProblemNotYetValid — сертификат ещё не действует (NotBefore
	// позже момента проверки).
	TLSProblemNotYetValid TLSProblem = "not_yet_valid"
	// TLSProblemHostnameMismatch — хост соединения (SNI/Host) не найден
	// ни в SAN, ни в CN (x509.HostnameError; см. verifyCertHostname).
	TLSProblemHostnameMismatch TLSProblem = "hostname_mismatch"
	// TLSProblemSelfSigned — лист подписан собственным ключом
	// (CheckSignature по собственной подписи): стандартная верификация
	// такой сертификат не пропускает — неизвестный издатель.
	TLSProblemSelfSigned TLSProblem = "self_signed"
)

// String возвращает код причины как строку ("expired", …).
func (p TLSProblem) String() string { return string(p) }

// TLSInfo — разбор сертификата установленного TLS-соединения для
// сохранения в результате проверки (ТЗ 5.5 п. 3: InsecureSkipVerify =
// true для коннекта — нужен контент, — но результат проверки
// сертификата, издатель, срок действия и SAN сохраняются; ТЗ 5.5 п. 7,
// колонка subdomain_results.tls jsonb — ТЗ 6.1). Собирается NewTLSInfo
// из tls.ConnectionState после рукопожатия; для соединения без TLS —
// nil, это не ошибка (§8).
//
// Механика самостоятельной верификации: соединение устанавливается с
// InsecureSkipVerify = true, а проверку транспорт выполняет сам по
// листовому сертификату (PeerCertificates[0] — при нескольких
// сертификатах сервера листом считается первый, mTLS-сценарии не
// отличаются): хост соединения в SAN/CN (VerifyHostname — SAN, при их
// отсутствии CN), окно действия NotBefore/NotAfter, самоподписанность.
// Цепочка доверия до системных корней не проверяется: результат не
// должен зависеть от хранилища корней контейнера, а некорректный
// сертификат загрузку не блокирует (§8) — атрибуты цепочки сохраняются
// информационно (ChainLength/ChainSubjects).
type TLSInfo struct {
	// Valid — лист прошёл все проверки (Problems пуст). «Осталось
	// дней» действия сертификата вычисляет потребитель из NotAfter —
	// хранить производные от времени значения смысла нет.
	Valid bool `json:"valid"`
	// Problems — причины провала проверки; пуст при Valid.
	Problems []TLSProblem `json:"problems,omitempty"`

	// Листовой сертификат.

	// SubjectCommonName, SubjectOrganization — субъект (CN, O).
	SubjectCommonName   string `json:"subject_cn,omitempty"`
	SubjectOrganization string `json:"subject_o,omitempty"`
	// IssuerCommonName, IssuerOrganization — издатель (CN, O).
	IssuerCommonName   string `json:"issuer_cn,omitempty"`
	IssuerOrganization string `json:"issuer_o,omitempty"`
	// SerialNumber — серийный номер, hex без ведущих нулей.
	SerialNumber string `json:"serial,omitempty"`
	// NotBefore, NotAfter — окно действия сертификата.
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after"`
	// SelfSigned — лист подписан собственным ключом.
	SelfSigned bool `json:"self_signed"`

	// SAN — subjectAltName листа: DNS-имена и IP-адреса.
	DNSNames    []string     `json:"dns_names,omitempty"`
	IPAddresses []netip.Addr `json:"ip_addresses,omitempty"`

	// Цепочка сервера в порядке передачи (лист первый): длина и
	// субъекты сертификатов в RFC 2253.
	ChainLength   int      `json:"chain_length"`
	ChainSubjects []string `json:"chain_subjects,omitempty"`
}

// NewTLSInfo формирует TLSInfo из состояния установленного соединения
// и выполняет самостоятельную проверку листового сертификата по host —
// хосту URL запроса (SNI/Host; домен или IP-литерал, без порта).
// cs == nil или без сертификатов сервера (http, анонимные шифры) —
// nil: отсутствие TLS не ошибка (§8). Вызывающие RoundTrip напрямую
// (http.Client поверх Fetcher, цепочки редиректов T-048) вызывают
// NewTLSInfo сами по resp.TLS и хосту итогнего URL.
func NewTLSInfo(cs *tls.ConnectionState, host string) *TLSInfo {
	return newTLSInfoAt(cs, host, time.Now())
}

// newTLSInfoAt — NewTLSInfo с заглушаемыми часами: детерминированные
// проверки expired/not_yet_valid в юнит-тестах.
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

	// Самостоятельная верификация (ТЗ 5.5 п. 3): порядок причин —
	// время действия, домен, самоподписанность; проверка продолжает
	// собирать причины после первой (потребитель видит всё).
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
	// Подпись листа сходится его же ключом — самоподписанность без
	// требования IsCA (CheckSignatureFrom отклоняет не-CA родителя
	// ConstraintViolationError ещё до проверки подписи).
	if err := leaf.CheckSignature(leaf.SignatureAlgorithm,
		leaf.RawTBSCertificate, leaf.Signature); err == nil {
		info.SelfSigned = true
		info.addProblem(TLSProblemSelfSigned)
	}
	info.Valid = len(info.Problems) == 0
	return info
}

// addProblem добавляет код причины провала проверки.
func (i *TLSInfo) addProblem(p TLSProblem) {
	if i.Problems == nil {
		i.Problems = make([]TLSProblem, 0, 1)
	}
	i.Problems = append(i.Problems, p)
}

// verifyCertHostname — домен хоста в SAN/CN сертификата. Основа —
// стандартный VerifyHostname (SAN, wildcard, IP-литералы). ТЗ 5.5 п. 3
// требует SAN/CN, а современный crypto/x509 CN игнорирует даже при
// отсутствии SAN, поэтому для сертификатов без dNSName-SAN имя
// сверяется с CommonName (RFC 6125 п. 6.4.4, поведение до эпохи SAN).
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

// firstString — первый элемент среза или "" (pkix.Organization — срез).
func firstString(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// IsTLSCertError классифицирует ошибку TLS-слоя (§8): вызвана ли она
// некорректным сертификатом или отказом протокола TLS, а не сетью.
// Сертификатные: ошибки верификации x509 (актуальны, когда вызывающий
// не выставил InsecureSkipVerify), отказ сервера предупреждением
// (AlertError), не-TLS ответ на TLS-порту (RecordHeaderError) и
// отсутствие системных корней. Сетевые (таймауты, обрыв соединения,
// отказ в подключении) — IsTimeout и потребителя; они не являются
// «некорректным сертификатом» и не блокируют сохранение данных
// соединения, установившегося до ошибки.
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

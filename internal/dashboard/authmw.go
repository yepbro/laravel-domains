// Авторизация дашборда (ТЗД §7): middleware Basic (§7.4 — имя
// constant-time, пароль — PBKDF2-сверка пакета auth, T-127),
// cookie-сессия `<expiry>.<HMAC-SHA256(expiry, производный ключ KDF)>`
// (§7.5 — быстрая проверка HMAC снимает стоимость KDF с периодического
// опроса) и per-IP защита от подбора (§7.6 — 5 неудач в минуту → 429
// до конца окна). Пароль, хэш и значения cookie в логи не пишутся
// (§9.2).
package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Параметры cookie-сессии (ТЗД §7.5) и защиты от подбора (ТЗД §7.6).
const (
	// sessionCookieName — имя cookie-сессии дашборда.
	sessionCookieName = "dashboard_session"
	// sessionTTL — срок сессии (ТЗД §7.5: 24 ч).
	sessionTTL = 24 * time.Hour
	// maxLoginFailures — неудачных сверок в окне до блокировки
	// (ТЗД §7.6: 5 в минуту).
	maxLoginFailures = 5
	// loginWindow — окно учёта неудач и блокировки (ТЗД §7.6: минута).
	loginWindow = time.Minute
)

// auth — middleware защищённых роутов (ТЗД §6.1). Порядок:
//
//  1. валидная cookie-сессия пропускает запрос без KDF (§7.5) и не
//     упирается в защиту от подбора — оператор с живой сессией не
//     заперт чужими неудачами со своего IP (общий NAT/прокси);
//  2. per-IP окно неудач (§7.6): 5 неудачных сверок в течение минуты —
//     429 + Retry-After до конца окна (проверка до KDF: заблокированный
//     адрес не расходует полсекунды процессора на каждую попытку);
//  3. Basic-сверка (§7.4): имя — subtle.ConstantTimeCompare, пароль —
//     PBKDF2 из формата T-127 constant-time; успех выдаёт свежую cookie
//     и сбрасывает окно неудач IP (зафиксированное решение по §7.6:
//     оператор, вошедший после опечаток, не остаётся у черты блокировки).
//
// Неудачная сверка — Warn с IP-адресом, без кредов (§9.2).
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := s.opt.Now()
		ip := clientIP(r)

		// (1) Cookie-сессия.
		if c, err := r.Cookie(sessionCookieName); err == nil && s.sessionValid(c.Value, now) {
			next(w, r)
			return
		}

		// (2) Защита от подбора.
		if ok, retry := s.limiter.check(ip, now); !ok {
			// Retry-After — остаток окна с округлением вверх, минимум 1 с.
			secs := int((retry + time.Second - 1) / time.Second)
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			http.Error(w, "too many failed login attempts", http.StatusTooManyRequests)
			return
		}

		// (3) Basic-сверка.
		user, pass, ok := r.BasicAuth()
		if !ok || !s.verifyCredentials(user, pass) {
			s.limiter.record(ip, now)
			s.opt.Log.Warn("неудачная сверка Basic-кредов дашборда", "ip", ip)
			w.Header().Set("WWW-Authenticate", `Basic realm="dashboard", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.limiter.reset(ip)
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookieName,
			Value:    s.newSession(now),
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			MaxAge:   int(sessionTTL.Seconds()),
		})
		next(w, r)
	}
}

// verifyCredentials — сверка Basic-кредов (§7.4): имя —
// subtle.ConstantTimeCompare, пароль — PasswordHash.Verify (KDF +
// ConstantTimeCompare, T-127). Обе сверки вычисляются всегда, без
// короткого замыкания: пропуск KDF при неверном имени выдавал бы его
// правильность временем ответа.
func (s *Server) verifyCredentials(user, pass string) bool {
	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(s.username)) == 1
	passOK := s.hash.Verify(pass)
	return userOK && passOK
}

// newSession — значение cookie-сессии (§7.5):
// `<expiry>.<HMAC-SHA256(expiry, производный ключ KDF)>`: expiry —
// unix-время окончания (десятичное), подпись — hex. Ключом служит
// ключ хэша (Key из строки T-127) — он равен производному ключу
// правильного пароля (Verify сверяет с ним Derive(password), §7.4),
// поэтому смена password_hash меняет ключ HMAC и инвалидирует все
// сессии — отдельный серверный секрет не нужен (§7.5).
func (s *Server) newSession(now time.Time) string {
	expiry := strconv.FormatInt(now.Add(sessionTTL).Unix(), 10)
	return expiry + "." + signSession(s.hash.Key, expiry)
}

// sessionValid — проверка cookie-сессии: компактный формат, будущий
// expiry, подпись constant-time (hmac.Equal). Любая ошибка — просто
// «нет сессии»: запрос продолжается Basic-сверкой.
func (s *Server) sessionValid(v string, now time.Time) bool {
	if len(v) > 128 { // 10-значный expiry + точка + 64 hex — с запасом
		return false
	}
	expiry, sig, ok := strings.Cut(v, ".")
	if !ok {
		return false
	}
	exp, err := strconv.ParseInt(expiry, 10, 64)
	if err != nil || now.Unix() >= exp {
		return false
	}
	return hmac.Equal([]byte(signSession(s.hash.Key, expiry)), []byte(sig))
}

// signSession — HMAC-SHA256(expiry, ключ) в hex.
func signSession(key []byte, expiry string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(expiry))
	return hex.EncodeToString(mac.Sum(nil))
}

// --- per-IP окно неудачных сверок (§7.6) --------------------------------------
//
// Образец — токен-бакет T-123 (internal/web/api.go): in-memory карта
// под мьютексом с ленивой уборкой (один инстанс сервиса, внешнего
// хранилища не нужно). Семантика — окно неудач, а не бакет с пополнением:
// «5 неудачных сверок в течение минуты — отказ до конца окна» не
// позволяет устойчивый подбор 5 попытками в минуту бесконечно: окно
// начинается первой неудачей пачки и истекает целиком.

type loginLimiter struct {
	max    int
	window time.Duration
	mu     sync.Mutex
	state  map[string]*loginWindowState
}

// loginWindowState — счётчик неудач IP внутри текущего окна.
type loginWindowState struct {
	fails int
	since time.Time // первая неудача окна
}

func newLoginLimiter(max int, window time.Duration) *loginLimiter {
	return &loginLimiter{max: max, window: window, state: make(map[string]*loginWindowState)}
}

// check — можно ли сверять креды с ip в момент now. Отказ несёт остаток
// окна для Retry-After; истёкшее окно открывает доступ (блокировка «до
// конца окна», не навсегда).
func (l *loginLimiter) check(ip string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state[ip]
	if !ok || st.fails < l.max {
		return true, 0
	}
	if remain := l.window - now.Sub(st.since); remain > 0 {
		return false, remain
	}
	return true, 0
}

// record — неудачная сверка: копит окно; окно старше минуты начинается
// заново. Заблокированные попытки окно не продлевают — иначе непрерывный
// поток 429 запирал бы IP навсегда вместо «до конца окна».
func (l *loginLimiter) record(ip string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state[ip]
	if !ok {
		st = &loginWindowState{since: now}
		l.state[ip] = st
		// Ленивая уборка: карта растёт с числом уникальных IP; записи
		// старше окон удаляются при проходе (авторизация — не hot path).
		if len(l.state) > 4096 {
			for k, v := range l.state {
				if now.Sub(v.since) > l.window*4 {
					delete(l.state, k)
				}
			}
		}
	} else if now.Sub(st.since) >= l.window {
		st.fails = 0
		st.since = now
	}
	st.fails++
}

// reset — успешная сверка сбрасывает окно IP (зафиксированное решение
// по §7.6).
func (l *loginLimiter) reset(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.state, ip)
}

// clientIP — адрес клиента для защиты от подбора и журнала неудач
// (§7.6/§9.2). За reverse-proxy (ТЗД §9.1) все соединения приходят с
// loopback nginx — тогда адрес оператора несёт X-Real-IP (образец nginx
// перезаписывает его значением $remote_addr). Заголовку доверяем только
// от loopback-пира и только валидному IP; иначе — RemoteAddr.
//
// Оговорка прямого доступа без прокси (тот же паттерн, что у формы
// лендинга T-123): пир с loopback-адреса может подставить произвольный
// X-Real-IP и тем самым менять «свой» IP от запроса к запросу. Это
// принято: в документированной топологии перед сервисом стоит nginx,
// перезаписывающий заголовок, а прямой dev-доступ — loopback самого
// оператора (доверенная сторона); в compose опубликованный порт даёт
// пира не-loopback, доверие не включается вовсе.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if pip := net.ParseIP(strings.TrimSpace(r.Header.Get("X-Real-IP"))); pip != nil {
			return pip.String()
		}
	}
	return host
}

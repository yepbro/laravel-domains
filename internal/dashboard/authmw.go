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
//     429 + Retry-After до конца окна; слот неудачи резервируется
//     атомарно с проверкой лимита и до KDF — заблокированный адрес не
//     расходует полсекунды процессора на каждую попытку, а параллельная
//     пачка запросов с одного IP не проходит к сверке вся целиком
//     (T-231: отдельные check и record вокруг KDF пропускали пачку
//     мимо лимита — его держали только последовательные попытки);
//  3. Basic-сверка (§7.4): имя — subtle.ConstantTimeCompare, пароль —
//     PBKDF2 из формата T-127 constant-time; успех выдаёт свежую cookie
//     и сбрасывает окно неудач IP — заодно списывается зарезервированный
//     слот (зафиксированное решение по §7.6: оператор, вошедший после
//     опечаток, не остаётся у черты блокировки).
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

		// (2) Защита от подбора: резервирование слота неудачи вместе
		// с проверкой лимита (T-231).
		if ok, retry := s.limiter.reserve(ip, now); !ok {
			// Retry-After — остаток окна с округлением вверх, минимум 1 с.
			secs := int((retry + time.Second - 1) / time.Second)
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			http.Error(w, "too many failed login attempts", http.StatusTooManyRequests)
			return
		}

		// (3) Basic-сверка. Слот неудачи уже зарезервирован (reserve):
		// провал оставляет его в окне, успех — сбрасывает окно целиком.
		user, pass, ok := r.BasicAuth()
		if !ok || !s.verifyCredentials(user, pass) {
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
			// Secure в TLS-режиме (T-227): при работе за прокси с TLS
			// браузер не уйдёт с сессией по plaintext при прямом заходе
			// мимо прокси; в plain-деплое не ставится — cookie с Secure
			// по http вообще не вернулась бы.
			Secure:   requestIsHTTPS(r),
			SameSite: http.SameSiteLaxMode,
			MaxAge:   int(sessionTTL.Seconds()),
		})
		next(w, r)
	}
}

// verifyCredentials — сверка Basic-кредов (§7.4): имя —
// subtle.ConstantTimeCompare, пароль — Verify (KDF +
// ConstantTimeCompare, T-127; поле verify — шов тестов, New ставит
// hash). Обе сверки вычисляются всегда, без короткого замыкания:
// пропуск KDF при неверном имени выдавал бы его правильность временем
// ответа.
func (s *Server) verifyCredentials(user, pass string) bool {
	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(s.username)) == 1
	passOK := s.verify.Verify(pass)
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

// loginWindowState — счётчик неудач IP внутри текущего окна. since —
// момент первой зарезервированной попытки окна (до фикса T-231 — первой
// записанной неудачи; сдвиг не больше длительности сверки, ~полсекунды).
type loginWindowState struct {
	fails int
	since time.Time
}

func newLoginLimiter(max int, window time.Duration) *loginLimiter {
	return &loginLimiter{max: max, window: window, state: make(map[string]*loginWindowState)}
}

// reserve — попытка сверки кредов с ip в момент now: слот неудачи
// занимается (инкремент счётчика) и проверяется лимит атомарно, под
// одним мьютексом (T-231) — параллельная пачка запросов с одного IP
// не проскакивает мимо лимита в зазоре между отдельными проверкой и
// записью. Отказ несёт остаток окна для Retry-After; окно, истёкшее
// целиком, начинается заново независимо от заполнения — «хвост»
// прошлых неудач не переносится в новую минуту (P1 ревью T-231;
// блокировка «до конца окна», не навсегда; забытый 429-ответ окно не
// продлевает — его слот не резервируется вовсе). Успешная сверка
// списывает зарезервированный слот сбросом окна (reset).
//
// Граница резервирования: слот занимается до сверки, поэтому сверх
// maxLoginFailures одновременных попыток с IP лишние получают 429 и с
// верными кредами (потолок одновременных сверок; осознанный компромисс
// варианта A — после первой успешной сверки клиент ходит с cookie и
// лимитера не касается, а успехи окно не копят: reset сразу).
func (l *loginLimiter) reserve(ip string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state[ip]
	if !ok {
		st = &loginWindowState{fails: 1, since: now}
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
		return true, 0
	}
	// Окно истекло целиком — новая попытка открывает следующее с чистым
	// счётчиком (сброс безусловно, не только при заполненном окне).
	if now.Sub(st.since) >= l.window {
		st.fails = 0
		st.since = now
	}
	if st.fails >= l.max {
		// Здесь окно ещё живо (иначе сброшено выше) — остаток положителен.
		return false, l.window - now.Sub(st.since)
	}
	st.fails++
	return true, 0
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

// requestIsHTTPS — запрос клиента пришёл по TLS (T-227): прямое
// TLS-соединение (r.TLS — дашборд так не слушает, оговорка на будущее)
// либо reverse-proxy терминирует TLS и передаёт схему клиента в
// X-Forwarded-Proto (образец nginx — docs/deploy.md §6; заголовку
// доверяем только от доверенного пира — тот же паттерн, что clientIP).
// Определяет флаг Secure cookie-сессии: без него cookie при работе за
// TLS ушла бы по plaintext при прямом доступе мимо прокси.
func requestIsHTTPS(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if !trustedPeer(r) {
		return false
	}
	// Список значений: последняя позиция — её дописал наш прокси
	// (по образцу X-Forwarded-For в internal/web, T-221); образец
	// nginx ставит одиночное значение $scheme.
	proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	return strings.EqualFold(strings.TrimSpace(proto[strings.LastIndex(proto, ",")+1:]), "https")
}

// trustedPeer — пир соединения является доверенным reverse-proxy этого
// хоста: loopback TCP (dashboard.listen за nginx, docs/deploy.md §6)
// либо unix-сокет. Сегодня дашборд слушает только TCP
// (dashboard.Run: net.Listen("tcp"), валидация конфига — [host]:port);
// unix-ветка — паритет с internal/web (T-221), чтобы при появлении
// unix-слушателя флаг Secure не пропал молча.
func trustedPeer(r *http.Request) bool {
	if _, ok := r.Context().Value(http.LocalAddrContextKey).(*net.UnixAddr); ok {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

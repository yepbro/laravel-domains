// Package auth — хранение и сверка пароля операторского дашборда
// (ТЗД §7.2–7.4, T-127): формат строки password_hash, генерация
// (crawlerctl dashboard-secret), разбор и сверка с постоянным временем.
// Пакет не знает про HTTP: middleware Basic и cookie-сессии — задача
// cmd/dashboard (T-128); для подписи cookie-сессии используется Derive —
// производный ключ KDF (ТЗД §7.5: смена пароля инвалидирует сессии).
//
// KDF — PBKDF2-HMAC-SHA256 из stdlib crypto/pbkdf2 (Go 1.24+, ТЗД §3.2:
// новых зависимостей нет). Формат строки (ТЗД §7.2, b64 — std encoding):
//
//	pbkdf2-sha256$<итерации>$<соль-b64>$<ключ-b64>
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

// Параметры схемы (ТЗД §7.2).
const (
	// Scheme — имя схемы в первой компоненте строки password_hash.
	Scheme = "pbkdf2-sha256"
	// DefaultIterations — рекомендуемое число итераций при генерации
	// (OWASP для PBKDF2-HMAC-SHA256; ТЗД §7.2).
	DefaultIterations = 600_000
	// MinSaltLen — минимальная соль в байтах (ТЗД §7.2: ≥ 16).
	MinSaltLen = 16
	// KeyLen — длина производного ключа в байтах (ТЗД §7.2: 32).
	KeyLen = 32
)

// Generate возвращает строку password_hash для пароля: PBKDF2-HMAC-SHA256,
// соль MinSaltLen байт из crypto/rand, ключ KeyLen байт. iterations <= 0 —
// DefaultIterations (младшие значения — тесты). Пустой пароль — ошибка:
// креды оператора обязательны (ТЗД §7.7).
func Generate(password string, iterations int) (string, error) {
	if password == "" {
		return "", fmt.Errorf("пароль пуст")
	}
	if iterations <= 0 {
		iterations = DefaultIterations
	}
	salt := make([]byte, MinSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("соль: %w", err)
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, iterations, KeyLen)
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

// PasswordHash — разобранная строка password_hash: параметры схемы и
// ожидаемый производный ключ. Неизменяемость не гарантируется — копия
// значения после Parse, а не защитная структура.
type PasswordHash struct {
	Iterations int
	Salt       []byte
	Key        []byte
}

// Parse разбирает строку формата ТЗД §7.2. Формат строгий: 4 компоненты,
// имя схемы, положительные целые итерации (читаются из строки — сверка
// считается с ними, ТЗД §7.2), соль ≥ MinSaltLen байт, ключ ровно KeyLen
// байт, base64 std encoding. Ошибки парсятся по компонентам — текст
// указывает, что именно неверно.
func Parse(s string) (PasswordHash, error) {
	parts := strings.Split(s, "$")
	if len(parts) != 4 || parts[0] != Scheme {
		return PasswordHash{}, fmt.Errorf("ожидается строка вида %s$<итерации>$<соль-b64>$<ключ-b64>", Scheme)
	}
	iter, err := strconv.Atoi(parts[1])
	if err != nil || iter < 1 {
		return PasswordHash{}, fmt.Errorf("итерации: ожидается положительное целое, получено %q", parts[1])
	}
	salt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return PasswordHash{}, fmt.Errorf("соль: base64: %w", err)
	}
	if len(salt) < MinSaltLen {
		return PasswordHash{}, fmt.Errorf("соль: минимум %d байт, получено %d", MinSaltLen, len(salt))
	}
	key, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return PasswordHash{}, fmt.Errorf("ключ: base64: %w", err)
	}
	if len(key) != KeyLen {
		return PasswordHash{}, fmt.Errorf("ключ: ровно %d байт, получено %d", KeyLen, len(key))
	}
	return PasswordHash{Iterations: iter, Salt: salt, Key: key}, nil
}

// Derive — производный ключ пароля: KDF с параметрами хэша (итерации и
// соль из строки, длина KeyLen). Тот же ключ — HMAC cookie-сессии
// дашборда (ТЗД §7.5). Ошибка возможна только при отрицательных
// параметрах (после Parse недостижимо).
func (h PasswordHash) Derive(password string) ([]byte, error) {
	return pbkdf2.Key(sha256.New, password, h.Salt, h.Iterations, KeyLen)
}

// Verify — сверка пароля с хэшем в постоянном времени
// (subtle.ConstantTimeCompare, ТЗД §7.4). Ошибка деривации (после Parse
// недостижима) — не совпадение.
func (h PasswordHash) Verify(password string) bool {
	key, err := h.Derive(password)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(key, h.Key) == 1
}

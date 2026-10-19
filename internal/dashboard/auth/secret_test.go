// Тесты хранения пароля дашборда (ТЗД §7.2–7.4): раундтрип
// «генерация → разбор → сверка», неверный пароль, строгий разбор
// формата, несовместимые параметры строки (итерации/длина ключа).
package auth

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
)

// fastIterations — итерации для тестов: раундтрип проверяет формат и
// деривацию, а не стоимость KDF (600 000 итераций ≈ полсекунды).
const fastIterations = 1000

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// Раундтрип: сгенерированная строка имеет форму ТЗД §7.2, разбирается
// с исходными параметрами и сверяется только со своим паролем.
func TestGenerateParseVerify(t *testing.T) {
	const password = "правильный пароль s3cret!"
	hash, err := Generate(password, fastIterations)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	parts := strings.Split(hash, "$")
	if len(parts) != 4 {
		t.Fatalf("хэш %q: ожидается 4 компоненты через $", hash)
	}
	if parts[0] != Scheme || parts[1] != "1000" {
		t.Errorf("хэш %q: ожидается %s$1000$…", hash, Scheme)
	}
	if salt, err := base64.StdEncoding.DecodeString(parts[2]); err != nil || len(salt) != MinSaltLen {
		t.Errorf("соль %q: ожидается base64 %d байт (err=%v)", parts[2], MinSaltLen, err)
	}
	if key, err := base64.StdEncoding.DecodeString(parts[3]); err != nil || len(key) != KeyLen {
		t.Errorf("ключ %q: ожидается base64 %d байт (err=%v)", parts[3], KeyLen, err)
	}

	ph, err := Parse(hash)
	if err != nil {
		t.Fatalf("Parse(%q): %v", hash, err)
	}
	if ph.Iterations != fastIterations || len(ph.Salt) != MinSaltLen || len(ph.Key) != KeyLen {
		t.Errorf("Parse: параметры %d/%d/%d, ожидаются %d/%d/%d",
			ph.Iterations, len(ph.Salt), len(ph.Key), fastIterations, MinSaltLen, KeyLen)
	}
	if !ph.Verify(password) {
		t.Errorf("Verify(свой пароль) = false, хочу true")
	}
	if ph.Verify("неверный пароль") {
		t.Errorf("Verify(чужой пароль) = true, хочу false")
	}
	if ph.Verify("") {
		t.Errorf("Verify(пустой пароль) = true, хочу false")
	}
}

// iterations <= 0 — DefaultIterations (600 000, OWASP; ТЗД §7.2).
// Сверка не повторяется: раундтрип сверен в TestGenerateParseVerify,
// здесь — только заявленные при генерации итерации.
func TestGenerateDefaultIterations(t *testing.T) {
	hash, err := Generate("пароль", 0)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	ph, err := Parse(hash)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if ph.Iterations != DefaultIterations {
		t.Errorf("итерации по умолчанию = %d, хочу %d", ph.Iterations, DefaultIterations)
	}
}

// Соль случайна: два хэша одного пароля различаются, оба сверяются.
func TestGenerateUniqueSalt(t *testing.T) {
	h1, err := Generate("пароль", fastIterations)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Generate("пароль", fastIterations)
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h2 {
		t.Errorf("два хэша одного пароля совпали — соль не случайна: %q", h1)
	}
	p1, p2 := mustParse(t, h1), mustParse(t, h2)
	if bytes.Equal(p1.Salt, p2.Salt) {
		t.Errorf("соли совпали: %x", p1.Salt)
	}
	if !p1.Verify("пароль") || !p2.Verify("пароль") {
		t.Errorf("оба хэша должны сверяться с паролем")
	}
}

// Пустой пароль не хэшируется: креды обязательны (ТЗД §7.7).
func TestGenerateEmptyPassword(t *testing.T) {
	if _, err := Generate("", fastIterations); err == nil {
		t.Errorf("Generate(пустой пароль): ожидалась ошибка")
	}
}

// Строгий разбор: битые варианты строки отвергаются с указанием
// компоненты (схема/итерации/соль/ключ).
func TestParseErrors(t *testing.T) {
	salt := b64(bytes.Repeat([]byte{0xAB}, MinSaltLen))
	key := b64(bytes.Repeat([]byte{0xAB}, KeyLen))
	valid := Scheme + "$1000$" + salt + "$" + key
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"пустая строка", "", "pbkdf2-sha256"},
		{"чужая схема", "pbkdf2-sha512$1000$" + salt + "$" + key, "pbkdf2-sha256"},
		{"без схемы", "1000$" + salt + "$" + key, "pbkdf2-sha256"},
		{"мало компонент", Scheme + "$1000$" + salt, "pbkdf2-sha256"},
		{"много компонент", valid + "$лишняя", "pbkdf2-sha256"},
		{"итерации не число", strings.Replace(valid, "$1000$", "$много$", 1), "итерации"},
		{"итерации ноль", strings.Replace(valid, "$1000$", "$0$", 1), "итерации"},
		{"итерации отрицательные", strings.Replace(valid, "$1000$", "$-1$", 1), "итерации"},
		{"соль не base64", strings.Replace(valid, salt, "не-base64!", 1), "соль"},
		{"соль короче 16 байт", strings.Replace(valid, salt, b64([]byte("short-salt")), 1), "соль"},
		{"ключ не base64", strings.Replace(valid, key, "??", 1), "ключ"},
		{"ключ 31 байт", strings.Replace(valid, key, b64(bytes.Repeat([]byte{1}, 31)), 1), "ключ"},
		{"ключ 33 байта", strings.Replace(valid, key, b64(bytes.Repeat([]byte{1}, 33)), 1), "ключ"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.in)
			if err == nil {
				t.Fatalf("Parse(%q): ожидалась ошибка, получен успех", tt.in)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Parse(%q) ошибка %q не указывает на %q", tt.in, err, tt.want)
			}
		})
	}
}

// Несовместимые параметры строки. Подменённые итерации синтаксически
// корректны (ТЗД §7.2: итерации читаются из строки) — но деривация
// другая, сверки нет; это защита от «обновления» стойкости правкой
// числа. Соль длиннее минимума — валидный формат (≥ 16 байт), сверка
// работает.
func TestVerifyIncompatibleParams(t *testing.T) {
	hash, err := Generate("пароль", fastIterations)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(hash, "$"+strconv.Itoa(fastIterations)+"$", "$"+strconv.Itoa(fastIterations+1)+"$", 1)
	ph, err := Parse(tampered)
	if err != nil {
		t.Fatalf("подмена итераций — валидный формат, Parse: %v", err)
	}
	if ph.Iterations != fastIterations+1 {
		t.Errorf("итерации из строки = %d, хочу %d", ph.Iterations, fastIterations+1)
	}
	if ph.Verify("пароль") {
		t.Errorf("хэш с чужими итерациями сверился с паролем — деривация обязана отличаться")
	}

	// Соль 24 байта (≥ минимума) — допустимый формат: сборка строки из
	// Derive сворачивается обратно.
	long := PasswordHash{Iterations: fastIterations, Salt: bytes.Repeat([]byte{7}, 24)}
	key, err := long.Derive("пароль")
	if err != nil {
		t.Fatal(err)
	}
	long.Key = key
	phLong, err := Parse(Scheme + "$" + strconv.Itoa(fastIterations) + "$" + b64(long.Salt) + "$" + b64(key))
	if err != nil {
		t.Fatalf("соль 24 байта — допустимый формат, Parse: %v", err)
	}
	if !phLong.Verify("пароль") {
		t.Errorf("хэш с солью 24 байта должен сверяться")
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

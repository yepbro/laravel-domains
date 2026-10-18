// Юнит-тесты crawlerctl check (T-119): вывод записей (редиректы, TLS,
// анализ правил с evidence), сводки ошибок схем, конвертации; run()
// без конфигурации — код 1 до соединений, неверный вход — код 2.
// Живой прогон конвейера (DNS → HTTP → анализ) — dev-стенд:
// crawlerctl check laravel-11-vite.crawl.test.
package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/model"
	"crawler/internal/rules"
)

// checkRecord — синтетическая запись для тестов вывода.
func checkRecord() *httpworker.Record {
	return &httpworker.Record{
		Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
		FinalURL:   "https://example.com/",
		HTTPStatus: 200,
		Redirects: []httpfetch.RedirectStep{
			{URL: "http://example.com/", Status: 301},
			{URL: "https://example.com/", Status: 200},
		},
		TLS: &httpfetch.TLSInfo{
			Valid: true, NotAfter: time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC),
			SubjectCommonName: "example.com", ChainLength: 3,
		},
		Body:    []byte(`<html>data</html>`),
		Header:  http.Header{"Content-Type": []string{"text/html"}},
		Cookies: []*http.Cookie{{Name: "XSRF-TOKEN", Value: "eyJpdiI6"}},
	}
}

// checkSet — минимальный набор для анализа синтетики (каталог во временном
// каталоге — rules.LoadDir, как боевой загрузчик).
func checkSet(t *testing.T) *rules.Set {
	t.Helper()
	dir := t.TempDir()
	body := "rules:\n" +
		"  - id: cookie_encrypted_payload\n" +
		"    target: cookie_value\n" +
		"    regex: '^eyJpdiI6'\n" +
		"    weight: 0.5\n" +
		"  - id: vite_assets\n" +
		"    target: html\n" +
		"    regex: '/build/assets/'\n" +
		"    weight: 0.2\n" +
		"    ver_min: \"9.19\"\n"
	if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	set, err := rules.LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	return set
}

// TestCheckPrintRecord — вывод записи: вариант/схема/статус/итоговый URL,
// цепочка редиректов без последнего шага, TLS-сводка, анализ с сигналом.
func TestCheckPrintRecord(t *testing.T) {
	var buf bytes.Buffer
	rec := checkRecord()
	rec.Body = []byte(`<script src="/build/assets/app-12345678.js"></script>`)
	set := checkSet(t)

	checkPrintRecord(&buf, context.Background(), rec, nil, set, 0.6)

	got := buf.String()
	for _, want := range []string{
		"запись main/https: 200 https://example.com/",
		"редиректы: 301→http://example.com/",
		"TLS: валиден, истекает", "CN \"example.com\", цепочка 3",
		"laravel: score=0.600 ver=[9.19..?] правил=2 is_laravel=true (исходный HTML)",
		"cookie_encrypted_payload", "eyJpdiI6",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("вывод не содержит %q:\n%s", want, got)
		}
	}
}

// TestCheckPrintRecordTLSProblems — невалидный TLS — перечень проблем.
func TestCheckPrintRecordTLSProblems(t *testing.T) {
	var buf bytes.Buffer
	rec := checkRecord()
	rec.TLS.Valid = false
	rec.TLS.Problems = []httpfetch.TLSProblem{"expired", "hostname_mismatch"}

	checkPrintRecord(&buf, context.Background(), rec, nil, checkSet(t), 0.6)
	if !strings.Contains(buf.String(), "ПРОБЛЕМЫ: expired, hostname_mismatch") {
		t.Errorf("проблемы TLS не выведены:\n%s", buf.String())
	}
}

// TestCheckPrintRecordError — запись с ошибкой класса вместо статуса.
func TestCheckPrintRecordError(t *testing.T) {
	var buf bytes.Buffer
	rec := checkRecord()
	rec.ErrClass = httpworker.ErrTimeout
	rec.HTTPStatus = 0

	checkPrintRecord(&buf, context.Background(), rec, nil, checkSet(t), 0.6)
	if !strings.Contains(buf.String(), "ошибка timeout") {
		t.Errorf("класс ошибки не в статусе:\n%s", buf.String())
	}
}

// TestCheckSchemeErrs — сводка ошибок схем субдомена без записей.
func TestCheckSchemeErrs(t *testing.T) {
	sub := &httpworker.SubdomainResult{
		HTTPS: httpworker.SchemeCheck{ErrClass: httpworker.ErrTLS},
		HTTP:  httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
	}
	if got := checkSchemeErrs(sub); got != "https: tls; http: timeout" {
		t.Errorf("сводка = %q, хочу обе ошибки", got)
	}
	sub.HTTP.ErrClass = httpworker.ErrNone
	// HTTP без ошибки класса, но и без ответа (Available=false) —
	// честное «нет ответа» рядом с ошибкой https.
	if got := checkSchemeErrs(sub); got != "https: tls; http: нет ответа" {
		t.Errorf("сводка = %q, хочу ошибку https и молчание http", got)
	}
	// Обе схемы ответили без ошибок — сводка деградирует до «нет ответа»
	// (записей при этом всё равно нет — аномальный для конвейера случай).
	sub.HTTPS.ErrClass = httpworker.ErrNone
	if got := checkSchemeErrs(sub); got == "" {
		t.Error("сводка пуста — вывод должен пояснять отсутствие записей")
	}
}

// TestCheckRuleCookies — конвертация http.Cookie в правила.
func TestCheckRuleCookies(t *testing.T) {
	cs := checkRuleCookies([]*http.Cookie{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}})
	if len(cs) != 2 || cs[0].Name != "a" || cs[1].Value != "2" {
		t.Errorf("конвертация = %+v", cs)
	}
}

// TestCheckAliasID — nil и значение.
func TestCheckAliasID(t *testing.T) {
	if got := checkAliasID(nil); got != "?" {
		t.Errorf("aliasID(nil) = %q", got)
	}
	v := int16(3)
	if got := checkAliasID(&v); got != "3" {
		t.Errorf("aliasID(3) = %q", got)
	}
}

// TestRunCheck — без конфигурации код 1 до соединений; неверный вход — 2.
func TestRunCheck(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"check", "example.com"}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "check: config:") {
		t.Fatalf("код = %d, stderr:\n%s; хочу 1 с ошибкой конфигурации", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout до соединения не пуст:\n%s", stdout.String())
	}
}

// TestRunCheckUsage — не один домен / мусорное имя — код 2.
func TestRunCheckUsage(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	for _, args := range [][]string{
		{"check"},
		{"check", "a.com", "b.com"},
		{"check", "не домен"},
		{"check", ""},
	} {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		if code != exitUsage {
			t.Errorf("run(%v) = %d, хочу %d; stderr:\n%s", args, code, exitUsage, stderr.String())
		}
	}
}

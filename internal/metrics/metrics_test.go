package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler(t *testing.T) {
	srv := httptest.NewServer(Handler(New("test-service")))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("статус = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain (формат Prometheus)", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		`crawler_service_up{service="test-service"} 1`,
		`crawler_build_info{mode="dev-stub",service="test-service"} 1`,
		"go_goroutines", // коллектор Go/runtime
	} {
		if !strings.Contains(text, want) {
			t.Errorf("в /metrics нет строки %q", want)
		}
	}
}

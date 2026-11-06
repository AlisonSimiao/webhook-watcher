package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWatcherLiveness(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	newLive := func() *watcherLiveness {
		l := newWatcherLiveness()
		l.now = func() time.Time { return now }
		return l
	}

	t.Run("sem watchers não é saudável", func(t *testing.T) {
		if healthy, _ := newLive().check(); healthy {
			t.Fatal("esperava não saudável sem nenhum watcher registrado")
		}
	})

	t.Run("watcher batendo é saudável", func(t *testing.T) {
		l := newLive()
		l.beat("DB01")
		healthy, watchers := l.check()
		if !healthy || watchers["DB01"] != "ok" {
			t.Fatalf("esperava saudável, obteve %v %v", healthy, watchers)
		}
	})

	t.Run("watcher sem bater além do limite está travado", func(t *testing.T) {
		l := newLive()
		l.beat("DB01")
		l.beat("DB02")
		now = now.Add(watcherStaleAfter + time.Second)
		l.beat("DB02")
		healthy, watchers := l.check()
		if healthy {
			t.Fatalf("esperava não saudável, obteve %v", watchers)
		}
		if watchers["DB02"] != "ok" {
			t.Fatalf("DB02 deveria continuar ok, obteve %q", watchers["DB02"])
		}
	})

	t.Run("watcher encerrado não é saudável", func(t *testing.T) {
		l := newLive()
		l.beat("DB01")
		l.stop("DB01")
		healthy, watchers := l.check()
		if healthy || watchers["DB01"] != "encerrado" {
			t.Fatalf("esperava encerrado, obteve %v %v", healthy, watchers)
		}
	})
}

func TestHealthHandler(t *testing.T) {
	for _, tc := range []struct {
		healthy    bool
		wantCode   int
		wantStatus string
	}{
		{true, http.StatusOK, "ok"},
		{false, http.StatusServiceUnavailable, "fail"},
	} {
		h := healthHandler(func() (bool, map[string]any) {
			return tc.healthy, map[string]any{"redis": "ok"}
		})
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

		if rec.Code != tc.wantCode {
			t.Fatalf("healthy=%v: status HTTP %d, esperava %d", tc.healthy, rec.Code, tc.wantCode)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("corpo não é JSON: %v", err)
		}
		if body["status"] != tc.wantStatus || body["redis"] != "ok" {
			t.Fatalf("corpo inesperado: %v", body)
		}
	}
}

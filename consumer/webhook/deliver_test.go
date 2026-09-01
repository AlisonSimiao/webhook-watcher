package webhook

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"webhook-watcher/config"
	"webhook-watcher/pkg/queue"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestDelivery_Handle_UnknownTableIsNoop(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	defer srv.Close()

	d := NewDelivery(srv.URL, "token", nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "clientes"} // "clientes" não está em tableToTipo
	if err := d.Handle(context.Background(), event); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("esperava nenhuma chamada HTTP, obteve %d", calls)
	}
}

func TestDelivery_Handle_PostsPayloadToInternoHooksTipo(t *testing.T) {
	var gotPath, gotAuth, gotTenant, gotContentType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotTenant = r.Header.Get("tenant")
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := NewDelivery(srv.URL, "srp-token-xyz", nil, testLogger())

	payload := []byte(`{"tipoModificacao":"M","recurso":{"id":42}}`)
	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos", Action: queue.ActionUpdate, Payload: payload}
	if err := d.Handle(context.Background(), event); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}

	if gotPath != "/interno/hooks/pedidos" {
		t.Fatalf("path inesperado: %s", gotPath)
	}
	if gotAuth != "srp-token-xyz" {
		t.Fatalf("header Authorization inesperado: %q (não deve ter prefixo Bearer)", gotAuth)
	}
	if gotTenant != "acme" {
		t.Fatalf("header tenant inesperado: %q", gotTenant)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type inesperado: %q", gotContentType)
	}
	if string(gotBody) != string(payload) {
		t.Fatalf("corpo enviado incorreto: esperava %s, obteve %s (deve ser Event.Payload cru, sem re-envelopar)", payload, gotBody)
	}
}

func TestDelivery_Handle_BaseURLTrailingSlashIsTrimmed(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := NewDelivery(srv.URL+"/", "token", nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos", Payload: []byte(`{}`)}
	if err := d.Handle(context.Background(), event); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}
	if gotPath != "/interno/hooks/pedidos" {
		t.Fatalf("path com barra dupla: %s", gotPath)
	}
}

func TestDelivery_Handle_NonSuccessStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	d := NewDelivery(srv.URL, "token-errado", nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos", Payload: []byte(`{}`)}
	if err := d.Handle(context.Background(), event); err == nil {
		t.Fatal("esperava erro para resposta 401, obteve nil")
	}
}

func TestDelivery_Handle_TransportErrorReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // conexão recusada

	d := NewDelivery(url, "token", nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos", Payload: []byte(`{}`)}
	if err := d.Handle(context.Background(), event); err == nil {
		t.Fatal("esperava erro de transporte, obteve nil")
	}
}

func TestDelivery_Handle_FailedDeliveriesOnlyPersistedOnLastAttempt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, err := config.InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	d := NewDelivery(srv.URL, "token", db, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos", Payload: []byte(`{}`)}
	// context.Background() não carrega metadados de task do asynq, então
	// isLastAttempt(ctx) é sempre false aqui — equivalente a "não é a última
	// tentativa" (o asynq só popula esses metadados dentro de um worker real).
	if err := d.Handle(context.Background(), event); err == nil {
		t.Fatal("esperava erro, obteve nil")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM failed_deliveries`).Scan(&count); err != nil {
		t.Fatalf("erro ao contar failed_deliveries: %v", err)
	}
	if count != 0 {
		t.Fatalf("esperava 0 linhas em failed_deliveries fora da última tentativa, obteve %d", count)
	}
}

func TestIsLastAttempt_PlainContextReturnsFalse(t *testing.T) {
	if isLastAttempt(context.Background()) {
		t.Fatal("esperava false para um context.Context sem metadados de task do asynq")
	}
}

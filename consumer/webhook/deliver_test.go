package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"webhook-watcher/config"
	"webhook-watcher/pkg/queue"
)

func cacheWithURLs(tenant, table string, urls ...string) *HookCache {
	c := NewHookCache(nil, nil, testLogger())
	tipo, ok := tableToTipo[table]
	if !ok {
		panic("tabela sem tradução conhecida em teste: " + table)
	}
	c.dest[destKey{tenant: tenant, tipo: tipo}] = urls
	return c
}

func TestDelivery_Handle_NoDestinationsIsNoop(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	defer srv.Close()

	cache := NewHookCache(nil, nil, testLogger()) // sem destinos cadastrados
	d := NewDelivery(cache, nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "semhook", Table: "pedidos"}
	if err := d.Handle(context.Background(), event); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("esperava nenhuma chamada HTTP, obteve %d", calls)
	}
}

func TestDelivery_Handle_PostsToAllConfiguredURLs(t *testing.T) {
	var received1, received2 []byte
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received1, _ = io.ReadAll(r.Body)
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type inesperado: %s", ct)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv1.Close()
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received2, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv2.Close()

	cache := cacheWithURLs("acme", "pedidos", srv1.URL, srv2.URL)
	d := NewDelivery(cache, nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos", Action: queue.ActionUpdate}
	if err := d.Handle(context.Background(), event); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}

	for _, body := range [][]byte{received1, received2} {
		var got queue.Event
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("corpo recebido não é um queue.Event válido: %v", err)
		}
		if got.ID != "evt_1" || got.Tenant != "acme" {
			t.Fatalf("evento recebido incorreto: %+v", got)
		}
	}
}

func TestDelivery_Handle_NonSuccessStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cache := cacheWithURLs("acme", "pedidos", srv.URL)
	d := NewDelivery(cache, nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos"}
	if err := d.Handle(context.Background(), event); err == nil {
		t.Fatal("esperava erro para resposta 500, obteve nil")
	}
}

func TestDelivery_Handle_TransportErrorReturnsError(t *testing.T) {
	// Servidor fechado imediatamente: URL válida, mas conexão recusada.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	cache := cacheWithURLs("acme", "pedidos", url)
	d := NewDelivery(cache, nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos"}
	if err := d.Handle(context.Background(), event); err == nil {
		t.Fatal("esperava erro de transporte, obteve nil")
	}
}

func TestDelivery_Handle_PartialFailureAmongMultipleDestinationsStillErrors(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer fail.Close()

	cache := cacheWithURLs("acme", "pedidos", ok.URL, fail.URL)
	d := NewDelivery(cache, nil, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos"}
	if err := d.Handle(context.Background(), event); err == nil {
		t.Fatal("esperava erro quando ao menos um destino falha, obteve nil")
	}
}

func TestDelivery_Handle_FailedDeliveriesOnlyPersistedOnLastAttempt(t *testing.T) {
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer fail.Close()

	db, err := config.InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	cache := cacheWithURLs("acme", "pedidos", fail.URL)
	d := NewDelivery(cache, db, testLogger())

	event := &queue.Event{ID: "evt_1", Tenant: "acme", Table: "pedidos"}
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

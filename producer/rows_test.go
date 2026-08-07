package producer

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/config"
	"webhook-watcher/pkg/queue"
)

func TestEachRow_OddRowCount(t *testing.T) {
	rs := &RowsStrategy{log: testLogger()}
	ev := &EventContext{
		Event: binlogUpdateEvent("meu_tenant", "pedidos", [][]interface{}{
			{int32(1)},
			pedidoRow(),
			{int32(2)}, // linha "old" órfã, sem par "new" correspondente
		}),
	}

	var visited []int
	err := rs.eachRow(ev, 2, 1, func(rowIndex int, newRow []interface{}) error {
		visited = append(visited, rowIndex)
		return nil
	})
	if err != nil {
		t.Fatalf("eachRow retornou erro inesperado: %v", err)
	}
	if len(visited) != 1 {
		t.Fatalf("esperava 1 linha completa visitada (a última incompleta deve ser ignorada), obteve %d: %v", len(visited), visited)
	}
}

// flakyQueue é um Enqueuer fake que falha nas N primeiras chamadas e depois
// sempre enfileira com sucesso — usado para testar o comportamento de emit
// diante de falhas de enqueue.
type flakyQueue struct {
	mu        sync.Mutex
	failCount int
	calls     int
	events    []*queue.Event
}

func (f *flakyQueue) Enqueue(_ context.Context, event *queue.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failCount {
		return errors.New("erro transitório simulado")
	}
	f.events = append(f.events, event)
	return nil
}

func (f *flakyQueue) Close() error { return nil }

func TestEmit_PersistsToFailedEventsOnEnqueueError(t *testing.T) {
	db, err := config.InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	fq := &flakyQueue{failCount: 999} // sempre falha
	rs := &RowsStrategy{log: testLogger(), enqueuer: fq, sqliteDB: db, serverID: "DB01"}

	event := &queue.Event{ID: "evt_test", Tenant: "meu_tenant", Table: "pedidos", Action: queue.ActionUpdate, Payload: []byte(`{"a":1}`)}
	rs.emit(context.Background(), event)

	if fq.calls != 1 {
		t.Fatalf("esperava exatamente 1 tentativa (sem retry), obteve %d", fq.calls)
	}
	if len(fq.events) != 0 {
		t.Fatalf("esperava nenhum evento enfileirado, obteve %d", len(fq.events))
	}

	row := db.QueryRow(`SELECT event_id, server_id, tenant, table_name, action, payload, error FROM failed_events WHERE event_id = ?`, "evt_test")
	var eventID, serverID, tenant, table, action, payload, errMsg string
	if err := row.Scan(&eventID, &serverID, &tenant, &table, &action, &payload, &errMsg); err != nil {
		t.Fatalf("evento não foi persistido em failed_events: %v", err)
	}
	if serverID != "DB01" || tenant != "meu_tenant" || table != "pedidos" || action != string(queue.ActionUpdate) {
		t.Fatalf("dados persistidos incorretos: server_id=%s tenant=%s table=%s action=%s", serverID, tenant, table, action)
	}
	if payload != `{"a":1}` {
		t.Fatalf("payload persistido incorreto: %s", payload)
	}
	if errMsg == "" {
		t.Fatalf("esperava mensagem de erro persistida, veio vazia")
	}
}

func TestEmit_SkipsPersistenceWhenSqliteDBNil(t *testing.T) {
	fq := &flakyQueue{failCount: 999} // sempre falha
	rs := &RowsStrategy{log: testLogger(), enqueuer: fq, sqliteDB: nil, serverID: "DB01"}

	rs.emit(context.Background(), &queue.Event{ID: "evt_test"})

	if fq.calls != 1 {
		t.Fatalf("esperava exatamente 1 tentativa, obteve %d", fq.calls)
	}
}

// binlogEventWithoutRowsEvent garante que eachRow trata com segurança um
// evento cujo payload não é um *replication.RowsEvent.
func TestEachRow_IgnoresNonRowsEvent(t *testing.T) {
	rs := &RowsStrategy{log: testLogger()}
	ev := &EventContext{
		Event: &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
			Event:  &replication.QueryEvent{},
		},
	}

	called := false
	err := rs.eachRow(ev, 2, 1, func(rowIndex int, newRow []interface{}) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("eachRow retornou erro inesperado: %v", err)
	}
	if called {
		t.Fatalf("visit não deveria ser chamado para evento que não é RowsEvent")
	}
}

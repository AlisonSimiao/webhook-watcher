package queue

import (
	"context"
	"testing"
)

func TestMemoryQueueEnqueueAndPending(t *testing.T) {
	m := NewMemoryQueue()
	ctx := context.Background()

	e1 := &Event{ID: "evt_1", Table: "pedidos"}
	e2 := &Event{ID: "evt_2", Table: "clientes"}

	if err := m.Enqueue(ctx, e1); err != nil {
		t.Fatalf("Enqueue(1) retornou erro: %v", err)
	}
	if err := m.Enqueue(ctx, e2); err != nil {
		t.Fatalf("Enqueue(2) retornou erro: %v", err)
	}

	pending := m.Pending()
	if len(pending) != 2 {
		t.Fatalf("esperava 2 pendentes, obteve %d", len(pending))
	}
	if pending[0].ID != "evt_1" || pending[1].ID != "evt_2" {
		t.Fatalf("ordem da fila não preservada: %v, %v", pending[0].ID, pending[1].ID)
	}

	// Pending devolve uma cópia da slice: appends na cópia não crescem a fila.
	pending = append(pending, &Event{ID: "injetado"})
	if len(m.Pending()) != 2 {
		t.Fatalf("Pending deve retornar uma cópia da slice")
	}
}

func TestMemoryQueueClose(t *testing.T) {
	m := NewMemoryQueue()
	ctx := context.Background()

	if err := m.Enqueue(ctx, &Event{ID: "evt_1"}); err != nil {
		t.Fatalf("Enqueue antes do Close retornou erro: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close retornou erro: %v", err)
	}

	if err := m.Enqueue(ctx, &Event{ID: "evt_2"}); err == nil {
		t.Fatalf("esperava erro ao enfileirar após Close")
	}
	if len(m.Pending()) != 0 {
		t.Fatalf("esperava fila vazia após Close, obteve %d", len(m.Pending()))
	}
}

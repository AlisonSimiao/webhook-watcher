package queue

import (
	"context"
	"errors"
	"sync"
)

// MemoryQueue é uma implementação de Enqueuer em memória para testes unitários
// e desenvolvimento sem Redis. Não persiste nada e não é compartilhada entre
// processos — não usar em produção.
type MemoryQueue struct {
	mu      sync.Mutex
	pending []*Event
	closed  bool
}

// NewMemoryQueue cria uma fila vazia em memória.
func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{}
}

// Enqueue adiciona o evento à fila em memória.
func (m *MemoryQueue) Enqueue(_ context.Context, event *Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("fila em memória encerrada")
	}
	m.pending = append(m.pending, event)
	return nil
}

// Close encerra a fila e descarta os eventos pendentes.
func (m *MemoryQueue) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.pending = nil
	return nil
}

// Pending devolve cópia dos eventos enfileirados (para asserções em testes).
func (m *MemoryQueue) Pending() []*Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Event, len(m.pending))
	copy(out, m.pending)
	return out
}

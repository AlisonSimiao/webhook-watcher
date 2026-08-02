// Package queue define o port de fila de eventos (Ports & Adapters) do Webhook
// Watcher. O core (binlog/producer) só conhece a interface Enqueuer — enfileira
// eventos sem saber o driver. Trocar o driver (asynq/Redis hoje, RabbitMQ
// amanhã) não altera a regra de negócio.
//
// Consumer side: o app registra um Handler tipado e o driver cuida de
// entrega, retry, backoff e dead-letter (no asynq, via arquivamento).
package queue

import (
	"context"
	"encoding/json"
)

// Action é o tipo de modificação detectado no binlog.
type Action string

const (
	ActionInsert Action = "INSERT"
	ActionUpdate Action = "UPDATE"
	ActionDelete Action = "DELETE"
)

// QueueName é o nome padrão da fila no driver (asynq usa como nome de fila).
const QueueName = "webhook-events"

// Event é o envelope transportado pela fila. Os campos de metadados são comuns
// a todo evento de webhook; Payload carrega o recurso enriquecido (ex:
// PedidoEvent do tables/) já serializado em JSON.
type Event struct {
	ID        string          `json:"id"`
	Tenant    string          `json:"tenant"`
	Table     string          `json:"table"`
	Action    Action          `json:"action"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// Enqueuer é o port de escrita da fila (producer side).
type Enqueuer interface {
	Enqueue(ctx context.Context, event *Event) error
	Close() error
}

// Handler processa um evento retirado da fila (consumer side). Erro retornado
// aciona o retry/backoff do driver; após esgotar as tentativas, o evento vai
// para o dead-letter (no asynq, fila archived).
type Handler func(ctx context.Context, event *Event) error

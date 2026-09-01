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
	"fmt"
)

// Action é o tipo de modificação detectado no binlog.
type Action string

const (
	ActionInsert Action = "INSERT"
	ActionUpdate Action = "UPDATE"
	ActionDelete Action = "DELETE"
)

// QueueName é o prefixo-base das filas de eventos. Cada tipo de consumer tem
// sua própria fila nomeada "<QueueName>.<tipo>" — asynq é uma fila de
// trabalho com competing-consumers: se dois tipos de consumer (ex: entrega
// HTTP e SSE) lessem da mesma fila, cada evento iria para só um deles. Definir
// os nomes aqui evita duplicar a string em cada arquivo.
const QueueName = "webhook-events"

// QueueNameHTTP é a fila do consumer de entrega de webhook via HTTP
// (consumer/webhook). Implementado nesta versão.
const QueueNameHTTP = QueueName + ".http"

// Extensão futura (NÃO implementado nesta versão): para um novo tipo de
// consumer (ex: SSE, notificações), adicione uma constante seguindo o mesmo
// padrão (ex: QueueNameSSE = QueueName + ".sse"), acrescente-a à lista de
// destinos passada ao FanoutQueue em main.go/initQueue, e crie um novo
// subcomando + RedisWorker apontando para essa fila (ver consumer_cmd.go).

// QueueNameHTTPShard nomeia a fila do shard N do consumer HTTP. A entrega
// HTTP precisa preservar a ordem de entrega por recurso, o que exige um
// worker de concorrência 1 por shard (ver ShardedQueue em sharded.go).
func QueueNameHTTPShard(shard int) string {
	return fmt.Sprintf("%s.%d", QueueNameHTTP, shard)
}

// Event é o envelope transportado pela fila. Os campos de metadados são comuns
// a todo evento de webhook; Payload carrega o recurso enriquecido (ex:
// PedidoEvent do tables/) já serializado em JSON.
type Event struct {
	ID         string          `json:"id"`
	Tenant     string          `json:"tenant"`
	Table      string          `json:"table"`
	Action     Action          `json:"action"`
	Timestamp  int64           `json:"timestamp"`
	ResourceID int             `json:"resource_id,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
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

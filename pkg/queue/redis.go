package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
)

// Opções de entrega. Retry, backoff exponencial e dead-letter (arquivamento)
// são gerenciados pelo asynq no lado do worker.
const (
	defaultMaxRetries  = 5
	defaultTaskTimeout = 30 * time.Second
)

// taskTypeEvent identifica o tipo de task no asynq.
const taskTypeEvent = "webhook:event"

// ErrDuplicate indica que o evento já está na fila (mesmo TaskID). É o erro
// retornado pelo Enqueue quando o binlog entrega o mesmo evento de novo (não há
// resume de posição entre restarts); pode ser tratado como no-op idempotente.
var ErrDuplicate = asynq.ErrTaskIDConflict

// RedisQueue é o adapter Redis (via asynq) do port Enqueuer.
type RedisQueue struct {
	client *asynq.Client
}

// NewRedisQueue cria o adapter Redis. addr é "host:port" do Redis.
func NewRedisQueue(addr string) *RedisQueue {
	return &RedisQueue{client: asynq.NewClient(asynq.RedisClientOpt{Addr: addr})}
}

// Enqueue publica o evento na fila. O Event.ID vira o TaskID do asynq, o que
// torna o enfileiramento idempotente: reentregas do mesmo evento (mesma
// posição de binlog) falham com ErrDuplicate.
func (q *RedisQueue) Enqueue(ctx context.Context, event *Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("erro ao serializar evento: %w", err)
	}

	task := asynq.NewTask(taskTypeEvent, payload,
		asynq.MaxRetry(defaultMaxRetries),
		asynq.Timeout(defaultTaskTimeout),
		asynq.Queue(QueueName),
	)
	if _, err := q.client.EnqueueContext(ctx, task, asynq.TaskID(event.ID)); err != nil {
		return fmt.Errorf("erro ao enfileirar evento %s: %w", event.ID, err)
	}
	return nil
}

// Close encerra a conexão com o Redis.
func (q *RedisQueue) Close() error {
	return q.client.Close()
}

// IsDuplicate informa se o erro é de evento já enfileirado (enqueue idempotente).
func IsDuplicate(err error) bool {
	return errors.Is(err, ErrDuplicate)
}

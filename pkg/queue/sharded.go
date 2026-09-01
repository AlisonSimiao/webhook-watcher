package queue

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
)

// ShardedQueue roteia cada evento para um único shard entre N, escolhido por
// hash determinístico de tenant:table:resourceID — mesmo recurso sempre cai
// no mesmo shard, preservando ordem de entrega por recurso quando o
// consumer daquele shard tem concorrência 1. Diferente de FanoutQueue (que
// replica para todos os destinos), aqui cada evento vai para só um.
type ShardedQueue struct {
	shards []Enqueuer
}

// NewShardedQueue recebe os Enqueuers de cada shard, na ordem 0..N-1.
func NewShardedQueue(shards ...Enqueuer) *ShardedQueue {
	return &ShardedQueue{shards: shards}
}

// shardFor escolhe o shard determinístico de um evento.
//
// IMPORTANTE: usa hash/fnv (estável) — NUNCA hash/maphash ou iteração de map
// (aleatórios por processo). Um hash não-determinístico entre restarts do
// produtor faria o mesmo recurso cair em shards diferentes ao longo do
// tempo, quebrando a garantia de ordem que é o único motivo do sharding
// existir.
func (s *ShardedQueue) shardFor(event *Event) Enqueuer {
	key := fmt.Sprintf("%s:%s:%d", event.Tenant, event.Table, event.ResourceID)
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.shards[h.Sum32()%uint32(len(s.shards))]
}

func (s *ShardedQueue) Enqueue(ctx context.Context, event *Event) error {
	return s.shardFor(event).Enqueue(ctx, event)
}

// Close fecha todos os shards, agregando eventuais erros.
func (s *ShardedQueue) Close() error {
	var errs []error
	for _, sh := range s.shards {
		if err := sh.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

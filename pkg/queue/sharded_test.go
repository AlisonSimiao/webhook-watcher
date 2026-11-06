package queue

import (
	"context"
	"errors"
	"testing"
)

type recordingTarget struct {
	events []*Event
	err    error
}

func (r *recordingTarget) Enqueue(_ context.Context, e *Event) error {
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, e)
	return nil
}

func (r *recordingTarget) Close() error { return r.err }

func newShards(n int) ([]*recordingTarget, *ShardedQueue) {
	shards := make([]*recordingTarget, n)
	targets := make([]Enqueuer, n)
	for i := range shards {
		shards[i] = &recordingTarget{}
		targets[i] = shards[i]
	}
	return shards, NewShardedQueue(targets...)
}

func TestShardedQueue_SameKeyAlwaysSameShard(t *testing.T) {
	shards, sq := newShards(8)
	event := &Event{Tenant: "meu_tenant", Table: "pedidos", ResourceID: 42}

	for i := 0; i < 20; i++ {
		if err := sq.Enqueue(context.Background(), event); err != nil {
			t.Fatalf("Enqueue retornou erro: %v", err)
		}
	}

	hit := 0
	for _, s := range shards {
		if len(s.events) > 0 {
			hit++
			if len(s.events) != 20 {
				t.Fatalf("esperava as 20 chamadas no mesmo shard, obteve %d", len(s.events))
			}
		}
	}
	if hit != 1 {
		t.Fatalf("esperava exatamente 1 shard recebendo eventos, obteve %d", hit)
	}
}

func TestShardedQueue_DifferentKeysDistribute(t *testing.T) {
	_, sq := newShards(8)
	hitShards := map[int]bool{}

	for id := 0; id < 50; id++ {
		event := &Event{Tenant: "meu_tenant", Table: "pedidos", ResourceID: id}
		target := sq.shardFor(event)
		for i, s := range sq.shards {
			if s == target {
				hitShards[i] = true
			}
		}
	}
	if len(hitShards) < 2 {
		t.Fatalf("esperava distribuição por mais de 1 shard com 50 chaves diferentes, obteve %d", len(hitShards))
	}
}

func TestShardedQueue_RegressionPin(t *testing.T) {
	_, sq := newShards(16)
	event := &Event{Tenant: "acme", Table: "pedidos", ResourceID: 123}

	target := sq.shardFor(event)
	gotIdx := -1
	for i, s := range sq.shards {
		if s == target {
			gotIdx = i
		}
	}

	// Pin de regressão: protege contra troca acidental do algoritmo de hash,
	// que quebraria a garantia de ordem em produção (o mesmo recurso passaria
	// a cair em shards diferentes ao longo do tempo). Valor calculado a partir
	// do FNV-1a 32-bit real de "acme:pedidos:123" (hash 1471620917 % 16 = 5).
	const wantIdx = 5
	if gotIdx != wantIdx {
		t.Fatalf("shard obtido para acme:pedidos:123 = %d, esperado %d (algoritmo de hash mudou?)", gotIdx, wantIdx)
	}
}

func TestShardedQueue_Close(t *testing.T) {
	_, sq := newShards(2)
	sq.shards[1] = &recordingTarget{err: errors.New("falha ao fechar")}

	if err := sq.Close(); err == nil {
		t.Fatal("esperava erro agregado de Close, obteve nil")
	}
}

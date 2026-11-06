package queue

import (
	"context"
	"errors"
	"fmt"
)

// FanoutQueue implementa Enqueuer publicando o mesmo Event em N filas, uma
// por tipo de consumer (ex: webhook-events.http, futuramente
// webhook-events.sse). Cada consumer type roda seu próprio processo/subcomando
// lendo só a sua fila — competing-consumers dentro do mesmo tipo continua
// funcionando, mas tipos diferentes recebem o evento cada um.
type FanoutQueue struct {
	targets []Enqueuer
}

// NewFanoutQueue recebe os Enqueuers de destino já construídos (ex: um
// *RedisQueue ou *ShardedQueue por tipo de consumer).
func NewFanoutQueue(targets ...Enqueuer) *FanoutQueue {
	return &FanoutQueue{targets: targets}
}

// Enqueue publica o evento em todos os destinos, sempre — um destino fora do
// ar não impede que os demais recebam o evento (sem short-circuit).
//
// Semântica de erro:
//   - se TODOS os destinos retornarem duplicidade (IsDuplicate), Enqueue
//     retorna ErrDuplicate — preserva o comportamento observado hoje pelo
//     chamador (RowsStrategy.emit trata isso como no-op).
//   - se ALGUM destino falhar com um erro que não seja duplicidade, Enqueue
//     retorna um erro agregado (via errors.Join) — RowsStrategy.emit,
//     inalterado, salva esse erro em failed_events.
func (f *FanoutQueue) Enqueue(ctx context.Context, event *Event) error {
	if len(f.targets) == 0 {
		return nil
	}
	var errs []error
	duplicates := 0
	for _, t := range f.targets {
		if err := t.Enqueue(ctx, event); err != nil {
			if IsDuplicate(err) {
				duplicates++
				continue
			}
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("fan-out: %d/%d destinos falharam: %w", len(errs), len(f.targets), errors.Join(errs...))
	}
	if duplicates == len(f.targets) {
		return ErrDuplicate
	}
	return nil
}

// Close fecha todos os destinos, agregando eventuais erros.
func (f *FanoutQueue) Close() error {
	var errs []error
	for _, t := range f.targets {
		if err := t.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

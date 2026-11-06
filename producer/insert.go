package producer

import (
	"webhook-watcher/pkg/queue"
)

// InsertRowsStrategy trata eventos de INSERT (uma linha por Recurso criado).
// O Recurso é enriquecido como no UPDATE e sai com o estado que existir na
// leitura — um pedido pode chegar sem itens, se eles ainda não foram gravados.
type InsertRowsStrategy struct {
	*RowsStrategy
}

func (s *InsertRowsStrategy) Handle(ctx *EventContext) error {
	s.log.Debug("Evento recebido", "tipo", ctx.Event.Header.EventType.String())
	return s.emitRows(ctx, queue.ActionInsert, 1, 0)
}

package producer

import (
	"webhook-watcher/pkg/queue"
)

// DeleteRowsStrategy trata eventos de DELETE (uma linha por Recurso removido).
// A linha deletada vai ao TableProcessor como OldRow: o Recurso de um Evento D
// é o último estado conhecido antes da remoção.
type DeleteRowsStrategy struct {
	*RowsStrategy
}

func (s *DeleteRowsStrategy) Handle(ctx *EventContext) error {
	s.log.Debug("Evento recebido", "tipo", ctx.Event.Header.EventType.String())
	return s.emitRows(ctx, queue.ActionDelete, 1, 0)
}

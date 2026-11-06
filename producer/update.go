package producer

import (
	"webhook-watcher/pkg/queue"
)

// UpdateRowsStrategy trata eventos de UPDATE (pares old/new).
type UpdateRowsStrategy struct {
	*RowsStrategy
}

func (s *UpdateRowsStrategy) Handle(ctx *EventContext) error {
	s.log.Debug("Evento recebido", "tipo", ctx.Event.Header.EventType.String())
	return s.emitRows(ctx, queue.ActionUpdate, 2, 1)
}

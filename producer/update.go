package producer

import (
	"github.com/go-mysql-org/go-mysql/replication"
)

// UpdateRowsStrategy trata eventos de UPDATE (pares old/new).
type UpdateRowsStrategy struct {
	RowsStrategy
}

func (s *UpdateRowsStrategy) Handle(ctx *EventContext) error {
	s.log.Debug("Evento recebido", "tipo", ctx.Event.Header.EventType.String())
	return s.eachRow(ctx, 2, 1, func(rowIndex int, newRow []interface{}) error {
		rowsEvent := ctx.Event.Event.(*replication.RowsEvent)
		event, ok := s.buildEvent(ctx, rowsEvent, ActionUpdate, rowIndex, newRow)
		if !ok {
			return nil
		}
		s.log.Info("Evento processado", "evento", event)
		return nil
	})
}

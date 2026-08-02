package producer

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

// UpdateRowsStrategy trata eventos de UPDATE (pares old/new).
type UpdateRowsStrategy struct {
	RowsStrategy
}

func (s *UpdateRowsStrategy) Handle(ctx *EventContext) error {
	fmt.Printf("Evento recebido: %s\n", ctx.Event.Header.EventType.String())
	return s.eachRow(ctx, 2, 1, func(rowIndex int, newRow []interface{}) error {
		rowsEvent := ctx.Event.Event.(*replication.RowsEvent)
		event, ok := s.buildEvent(ctx, rowsEvent, ActionUpdate, rowIndex, newRow)
		if !ok {
			return nil
		}
		fmt.Println(event)
		return nil
	})
}

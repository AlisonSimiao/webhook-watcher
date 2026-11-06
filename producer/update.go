package producer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/pkg/queue"
)

// UpdateRowsStrategy trata eventos de UPDATE (pares old/new).
type UpdateRowsStrategy struct {
	RowsStrategy
}

func (s *UpdateRowsStrategy) Handle(ctx *EventContext) error {
	s.log.Debug("Evento recebido", "tipo", ctx.Event.Header.EventType.String())
	return s.eachRow(ctx, 2, 1, func(rowIndex int, newRow []interface{}) error {
		rowsEvent := ctx.Event.Event.(*replication.RowsEvent)
		schema := string(rowsEvent.Table.Schema)
		tableName := strings.ToLower(string(rowsEvent.Table.Table))

		resourceID, ok := rowResourceID(newRow)
		if !ok {
			tipo := "linha vazia"
			if len(newRow) > 0 {
				tipo = fmt.Sprintf("%T", newRow[0])
			}
			s.log.Warn("Coluna 0 não é INT assinado; evento ignorado",
				"tenant", schema,
				"table", tableName,
				"tipo", tipo)
			return nil
		}

		// A dataEvento é lida antes do Process, que é quem lê o estado do
		// Recurso no MariaDB (enriquecimento): o snapshot é no máximo tão novo
		// quanto ela, nunca mais antigo.
		dataEvento := s.dataEvento()

		// Apenas tabelas com TableProcessor registrado geram eventos.
		tipo, recurso, handled, err := s.dispatchTable(schema, tableName, string(queue.ActionUpdate), newRow, nil)
		if err != nil {
			s.log.Error("Erro ao processar tabela com TableProcessor", "tabela", tableName, "error", err)
			return err
		}
		if !handled {
			s.log.Debug("Tabela sem TableProcessor; nenhum evento emitido", "tabela", tableName)
			return nil
		}

		recursoJSON, err := json.Marshal(recurso)
		if err != nil {
			s.log.Error("Erro ao serializar recurso do evento", "tabela", tableName, "error", err)
			return err
		}

		id := generateEventID(ctx.BinlogFile, ctx.BinlogPos, rowIndex, schema, tableName, resourceID)
		payload, err := json.Marshal(queue.Contrato{
			IDEvento:        id,
			Tipo:            tipo,
			TipoModificacao: queue.TipoModificacaoDe(queue.ActionUpdate),
			DataEvento:      dataEvento,
			Tenant:          schema,
			Recurso:         recursoJSON,
		})
		if err != nil {
			s.log.Error("Erro ao serializar payload do evento", "tabela", tableName, "error", err)
			return err
		}

		event := &queue.Event{
			ID:         id,
			Tenant:     schema,
			Table:      tableName,
			Action:     queue.ActionUpdate,
			ResourceID: resourceID,
			Payload:    payload,
		}
		s.emit(ctx.Ctx, event)
		return nil
	})
}

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

type Producer struct{}

type Action string

const (
	ActionInsert Action = "INSERT"
	ActionUpdate Action = "UPDATE"
	ActionDelete Action = "DELETE"
)

type Event struct {
	ID         string `json:"id"`
	ResourceID int    `json:"resource_id"`
	Tenant     string `json:"tenant"`
	Action     Action `json:"action"`
	Table      string `json:"table"`
	Timestamp  int64  `json:"timestamp"`
}

func NewProducer() *Producer {
	return &Producer{}
}

func generateEventID(binlogFile string, logPos uint32, rowIndex int, event Event) string {
	raw := fmt.Sprintf("%s:%d:%d:%s:%s:%d", binlogFile, logPos, rowIndex, event.Tenant, event.Table, event.ResourceID)
	hash := sha256.Sum256([]byte(raw))

	return "evt_" + hex.EncodeToString(hash[:16])
}

func (p *Producer) HandleEvent(binlogFile string, binlogPos uint32, ev *replication.BinlogEvent) error {
	switch ev.Header.EventType {
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		fmt.Printf("Evento recebido: %s\n", ev.Header.EventType.String())
		rowsEvent, ok := ev.Event.(*replication.RowsEvent)

		if !ok {
			return nil
		}

		rowIndex := 0
		for i := 0; i < len(rowsEvent.Rows); i += 2 {
			newRow := rowsEvent.Rows[i+1]
			resourceID, ok := newRow[0].(int32)
			if !ok {
				fmt.Printf("Coluna 0 de %s.%s não é INT assinado (tipo %T); evento ignorado\n",
					rowsEvent.Table.Schema, rowsEvent.Table.Table, newRow[0])
				continue
			}
			event := Event{
				ResourceID: int(resourceID),
				Tenant:     string(rowsEvent.Table.Schema),
				Action:     ActionUpdate,
				Table:      string(rowsEvent.Table.Table),
				Timestamp:  int64(ev.Header.Timestamp),
			}

			event.ID = generateEventID(binlogFile, binlogPos, rowIndex, event)

			fmt.Println(event)

		}
	default:
		fmt.Printf("Evento recebido: %s\n", ev.Header.EventType.String())
	}

	return nil
}

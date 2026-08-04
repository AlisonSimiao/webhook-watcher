package producer

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/pkg/queue"
	"webhook-watcher/tables/pedido"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func binlogUpdateEvent(schema, table string, rows [][]interface{}) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: 1780000000,
			EventType: replication.UPDATE_ROWS_EVENTv2,
			LogPos:    456,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte(schema), Table: []byte(table)},
			Rows:  rows,
		},
	}
}

func pedidoRow() []interface{} {
	row := make([]interface{}, 25)
	row[0] = int32(42)   // id
	row[1] = "PED-00042" // codigo
	row[4] = int32(1)    // status
	row[5] = "1"         // codigoStatusErp
	row[24] = int32(1)   // aceitaFaturamentoParcial
	return row
}

func TestUpdateStrategyEmitsCustomEvent(t *testing.T) {
	mem := queue.NewMemoryQueue()
	prod := NewProducer(testLogger(), nil, mem)

	err := prod.HandleEvent("mysql-bin.000001", 123, binlogUpdateEvent("meu_tenant", "pedidos", [][]interface{}{
		{int32(42)},
		pedidoRow(),
	}))
	if err != nil {
		t.Fatalf("HandleEvent retornou erro: %v", err)
	}

	pending := mem.Pending()
	if len(pending) != 1 {
		t.Fatalf("esperava 1 evento na fila, obteve %d", len(pending))
	}
	event := pending[0]
	if event.Tenant != "meu_tenant" {
		t.Errorf("Tenant inesperado: %s", event.Tenant)
	}
	if event.Table != "pedidos" {
		t.Errorf("Table inesperado: %s", event.Table)
	}
	if event.Action != queue.ActionUpdate {
		t.Errorf("Action inesperado: %s", event.Action)
	}
	if event.Timestamp != 1780000000 {
		t.Errorf("Timestamp inesperado: %d", event.Timestamp)
	}
	if len(event.ID) == 0 || event.ID[:4] != "evt_" {
		t.Errorf("ID malformado: %s", event.ID)
	}

	var payload pedido.PedidoEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("payload não é um PedidoEvent válido: %v", err)
	}
	if payload.TipoModificacao != pedido.TipoModificacaoModificado {
		t.Errorf("TipoModificacao inesperado: %s", payload.TipoModificacao)
	}
	if payload.Recurso.ID != 42 {
		t.Errorf("Recurso.ID inesperado: %d", payload.Recurso.ID)
	}
	if payload.Recurso.Codigo != "PED-00042" {
		t.Errorf("Recurso.Codigo inesperado: %s", payload.Recurso.Codigo)
	}
}

func TestUpdateStrategySkipsUnregisteredTable(t *testing.T) {
	mem := queue.NewMemoryQueue()
	prod := NewProducer(testLogger(), nil, mem)

	err := prod.HandleEvent("mysql-bin.000001", 123, binlogUpdateEvent("meu_tenant", "clientes", [][]interface{}{
		{int32(1)},
		{int32(1), "CLI-1"},
	}))
	if err != nil {
		t.Fatalf("HandleEvent retornou erro: %v", err)
	}
	if len(mem.Pending()) != 0 {
		t.Fatalf("esperava nenhum evento para tabela sem processor, obteve %d", len(mem.Pending()))
	}
}

func TestUpdateStrategySkipsNonIntID(t *testing.T) {
	mem := queue.NewMemoryQueue()
	prod := NewProducer(testLogger(), nil, mem)

	err := prod.HandleEvent("mysql-bin.000001", 123, binlogUpdateEvent("meu_tenant", "pedidos", [][]interface{}{
		{"nao-int"},
		{"nao-int", "PED-1"},
	}))
	if err != nil {
		t.Fatalf("HandleEvent retornou erro: %v", err)
	}
	if len(mem.Pending()) != 0 {
		t.Fatalf("esperava nenhum evento com coluna 0 não inteira, obteve %d", len(mem.Pending()))
	}
}

func TestGenerateEventIDDeterministic(t *testing.T) {
	a := generateEventID("mysql-bin.000001", 100, 1, "meu_tenant", "pedidos", 42)
	b := generateEventID("mysql-bin.000001", 100, 1, "meu_tenant", "pedidos", 42)
	if a != b {
		t.Errorf("ID não determinístico: %s != %s", a, b)
	}
	if len(a) != len("evt_")+32 {
		t.Errorf("ID com tamanho inesperado: %s", a)
	}

	c := generateEventID("mysql-bin.000001", 100, 1, "meu_tenant", "pedidos", 43)
	if a == c {
		t.Errorf("IDs distintos deveriam diferir para resource_id diferentes")
	}
}

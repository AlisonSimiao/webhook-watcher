package producer

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/pkg/queue"
	"webhook-watcher/tables"
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

func TestUpdateStrategyEmitsCanonicalContract(t *testing.T) {
	mem := queue.NewMemoryQueue()
	prod := NewProducer(testLogger(), nil, nil, "test-server", mem)
	leitura := time.Date(2026, 11, 6, 11, 3, 12, 457_000_000, time.FixedZone("BRT", -3*60*60))
	prod.update.now = func() time.Time { return leitura }

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
	if len(event.ID) == 0 || event.ID[:4] != "evt_" {
		t.Errorf("ID malformado: %s", event.ID)
	}

	var contrato queue.Contrato
	if err := json.Unmarshal(event.Payload, &contrato); err != nil {
		t.Fatalf("payload não é um Contrato válido: %v", err)
	}
	if contrato.IDEvento != event.ID {
		t.Errorf("IDEvento inesperado: %s (esperava %s)", contrato.IDEvento, event.ID)
	}
	if contrato.Tipo != "pedidos" {
		t.Errorf("Tipo inesperado: %s", contrato.Tipo)
	}
	if contrato.TipoModificacao != queue.TipoModificacaoModificado {
		t.Errorf("TipoModificacao inesperado: %s", contrato.TipoModificacao)
	}
	// dataEvento = instante da leitura do estado, em UTC com milissegundos.
	if contrato.DataEvento != "2026-11-06T14:03:12.457Z" {
		t.Errorf("DataEvento inesperado: %s", contrato.DataEvento)
	}
	if contrato.Tenant != "meu_tenant" {
		t.Errorf("Tenant inesperado: %s", contrato.Tenant)
	}

	var recurso pedido.PedidoRecurso
	if err := json.Unmarshal(contrato.Recurso, &recurso); err != nil {
		t.Fatalf("recurso não é um PedidoRecurso válido: %v", err)
	}
	if recurso.ID != 42 {
		t.Errorf("Recurso.ID inesperado: %d", recurso.ID)
	}
	if recurso.Codigo != "PED-00042" {
		t.Errorf("Recurso.Codigo inesperado: %s", recurso.Codigo)
	}
}

func TestDataEventoTakenBeforeEnrichment(t *testing.T) {
	mem := queue.NewMemoryQueue()
	prod := NewProducer(testLogger(), nil, nil, "test-server", mem)

	var ordem []string
	prod.update.now = func() time.Time {
		ordem = append(ordem, "relogio")
		return time.Unix(0, 0)
	}
	prod.update.processors = []tables.TableProcessor{&processorEspiao{ordem: &ordem}}

	if err := prod.HandleEvent("mysql-bin.000001", 123, binlogUpdateEvent("meu_tenant", "pedidos", [][]interface{}{
		{int32(42)},
		pedidoRow(),
	})); err != nil {
		t.Fatalf("HandleEvent retornou erro: %v", err)
	}
	if len(ordem) != 2 || ordem[0] != "relogio" || ordem[1] != "process" {
		t.Fatalf("dataEvento deve ser lida antes do Process (enriquecimento); ordem: %v", ordem)
	}
}

// processorEspiao registra quando Process é chamado, para verificar que a
// dataEvento é capturada antes da leitura do estado.
type processorEspiao struct{ ordem *[]string }

func (p *processorEspiao) Supports(string) bool { return true }
func (p *processorEspiao) Tipo() string         { return "pedidos" }
func (p *processorEspiao) Process(*tables.TableContext) (interface{}, error) {
	*p.ordem = append(*p.ordem, "process")
	return map[string]int{"id": 42}, nil
}

func TestUpdateStrategySkipsUnregisteredTable(t *testing.T) {
	mem := queue.NewMemoryQueue()
	prod := NewProducer(testLogger(), nil, nil, "test-server", mem)

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
	prod := NewProducer(testLogger(), nil, nil, "test-server", mem)

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

func TestUpdateStrategySkipsEmptyRow(t *testing.T) {
	mem := queue.NewMemoryQueue()
	prod := NewProducer(testLogger(), nil, nil, "test-server", mem)

	err := prod.HandleEvent("mysql-bin.000001", 123, binlogUpdateEvent("meu_tenant", "pedidos", [][]interface{}{
		{int32(1)},
		{}, // linha "new" vazia: não deve dar panic ao indexar newRow[0]
	}))
	if err != nil {
		t.Fatalf("HandleEvent retornou erro: %v", err)
	}
	if len(mem.Pending()) != 0 {
		t.Fatalf("esperava nenhum evento para linha vazia, obteve %d", len(mem.Pending()))
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

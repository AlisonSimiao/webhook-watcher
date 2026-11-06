package pedido

import (
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"webhook-watcher/tables"
)

// No DELETE a linha já não existe no MariaDB: o Recurso é a capa como estava
// na linha deletada, sem consultas de enriquecimento. O *sql.DB zerado faz
// panic em qualquer consulta, então o teste falha se o Process enriquecer.
func TestProcessDeleteUsesDeletedRowWithoutEnrichment(t *testing.T) {
	row := make([]interface{}, 20)
	row[0] = int32(42)
	row[1] = "PED-00042"
	row[19] = "7" // id_vendedor: dispararia a busca do representante no enriquecimento

	ctx := &tables.TableContext{
		TableName: "pedidos",
		Schema:    "meu_tenant",
		Action:    "DELETE",
		OldRow:    row,
		DB:        &sql.DB{}, // não-nil: qualquer consulta faria panic
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	res, err := NewPedidoProcessor().Process(ctx)
	if err != nil {
		t.Fatalf("Process retornou erro: %v", err)
	}
	recurso := res.(PedidoRecurso)
	if recurso.ID != 42 || recurso.Codigo != "PED-00042" || recurso.CodigoRepresentante != "7" {
		t.Errorf("Recurso não veio da linha deletada: %+v", recurso)
	}
}

package tables

import (
	"database/sql"
	"log/slog"
)

// TableContext reúne as informações transmitidas pelo binlog para o processador de tabela.
type TableContext struct {
	TableName string
	Schema    string
	Action    string // "INSERT", "UPDATE", "DELETE"
	NewRow    []interface{}
	OldRow    []interface{}
	DB        *sql.DB
	Log       *slog.Logger
}

// TableProcessor é a interface que cada tabela/recurso deve implementar.
type TableProcessor interface {
	Supports(tableName string) bool
	Process(ctx *TableContext) (interface{}, error)
}

package pedido

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

type PedidoEnricher struct {
	db     *sql.DB
	schema string
	log    *slog.Logger
}

func NewPedidoEnricher(db *sql.DB, schema string, logger *slog.Logger) *PedidoEnricher {
	return &PedidoEnricher{
		db:     db,
		schema: schema,
		log:    logger,
	}
}

// tableName qualifica o nome da tabela com o schema do binlog (tenant).
func (e *PedidoEnricher) tableName(name string) string {
	if e.schema == "" {
		return name
	}
	return fmt.Sprintf("`%s`.`%s`", strings.ReplaceAll(e.schema, "`", "``"), name)
}

func (e *PedidoEnricher) Enrich(recurso *PedidoRecurso) {
	if e.db == nil {
		return
	}

	if recurso.CodigoRepresentante != "" {
		var tenant, email, whatsapp string
		var tipo int
		queryUser := fmt.Sprintf("SELECT tenant, email, whatsapp, tipo FROM %s WHERE id = ? AND tipo = 2 LIMIT 1", e.tableName("usuarios"))
		err := e.db.QueryRow(queryUser, recurso.CodigoRepresentante).Scan(&tenant, &email, &whatsapp, &tipo)
		if err != nil {
			e.log.Warn("Não foi possível buscar representante do pedido", "id_vendedor", recurso.CodigoRepresentante, "error", err)
		} else {
			recurso.Tenant = tenant
			recurso.EmailRepresentante = email
			recurso.WhatsappRepresentante = whatsapp
			recurso.TipoRepresentante = tipo
		}
	}

	if recurso.Codigo != "" || recurso.Numero > 0 {
		pedId := recurso.Codigo
		if pedId == "" {
			pedId = fmt.Sprintf("%d", recurso.Numero)
		}

		queryItens := fmt.Sprintf("SELECT sequencia, codigo_produto, codigo_empresa, codigo_cor, codigo_unidade_medida, "+
			"quantidade, preco_bruto, preco_liquido, codigo_integracao "+
			"FROM %s WHERE codigo_pedido = ?", e.tableName("pedido_item"))
		rows, err := e.db.Query(queryItens, pedId)
		if err != nil {
			e.log.Warn("Não foi possível buscar itens do pedido", "codigo_pedido", pedId, "error", err)
		} else {
			defer rows.Close()
			var itens []PedidoItem
			for rows.Next() {
				var item PedidoItem
				if errScan := rows.Scan(
					&item.Sequencia,
					&item.CodigoProduto,
					&item.CodigoEmpresa,
					&item.CodigoCor,
					&item.CodigoUnidadeMedida,
					&item.Quantidade,
					&item.PrecoBruto,
					&item.PrecoLiquido,
					&item.CodigoIntegracao,
				); errScan != nil {
					e.log.Warn("Não foi possível escanear item do pedido", "codigo_pedido", pedId, "error", errScan)
					continue
				}
				itens = append(itens, item)
			}
			if err := rows.Err(); err != nil {
				e.log.Warn("Erro ao iterar itens do pedido", "codigo_pedido", pedId, "error", err)
			}
			if len(itens) > 0 {
				recurso.Itens = itens
			}
		}
	}
}

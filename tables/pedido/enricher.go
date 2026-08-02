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

	if recurso.ID > 0 {
		// Colunas mapeadas do schema real de pedidos_produtos (COALESCE para
		// NULLs). codigo_empresa/codigo_cor/codigo_unidade_medida não existem na
		// tabela — exigiriam JOINs (produtos_representadas, produtos_opcoes_item).
		queryItens := fmt.Sprintf(`SELECT COALESCE(sequencia, 0), COALESCE(codigo, ''),
			COALESCE(codigo_integracao, ''), COALESCE(quantidade, 0),
			COALESCE(preco, 0), COALESCE(preco_original, 0),
			COALESCE(faturado, 0), COALESCE(cancelado, 0), COALESCE(peso, 0),
			COALESCE(desconto, 0), COALESCE(icms_valor, 0), COALESCE(ipi_valor, 0),
			COALESCE(informacoes, ''), data_emissao, COALESCE(previsao_faturamento, '')
			FROM %s WHERE id_pedido = ?`, e.tableName("pedidos_produtos"))
		rows, err := e.db.Query(queryItens, recurso.ID)
		if err != nil {
			e.log.Warn("Não foi possível buscar itens do pedido", "id_pedido", recurso.ID, "error", err)
		} else {
			defer rows.Close()
			var itens []PedidoItem
			for rows.Next() {
				var item PedidoItem
				if errScan := rows.Scan(
					&item.Sequencia,
					&item.CodigoProduto,
					&item.CodigoIntegracao,
					&item.Quantidade,
					&item.PrecoLiquido,
					&item.PrecoBruto,
					&item.Faturado,
					&item.Cancelado,
					&item.Peso,
					&item.Desconto,
					&item.Icms,
					&item.Ipi,
					&item.Informacoes,
					&item.DataEmissao,
					&item.DataPrevisaoFaturamento,
				); errScan != nil {
					e.log.Warn("Não foi possível escanear item do pedido", "id_pedido", recurso.ID, "error", errScan)
					continue
				}
				itens = append(itens, item)
			}
			if err := rows.Err(); err != nil {
				e.log.Warn("Erro ao iterar itens do pedido", "id_pedido", recurso.ID, "error", err)
			}
			if len(itens) > 0 {
				recurso.Itens = itens
			}
		}
	}
}

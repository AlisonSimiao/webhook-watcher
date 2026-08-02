package pedido

import (
	"fmt"
	"strings"
	"webhook-watcher/tables"
)

type PedidoProcessor struct{}

func NewPedidoProcessor() *PedidoProcessor {
	return &PedidoProcessor{}
}

func (p *PedidoProcessor) Supports(tableName string) bool {
	return strings.ToLower(tableName) == "pedidos"
}

func (p *PedidoProcessor) Process(ctx *tables.TableContext) (interface{}, error) {
	recurso := parsePedidoRecursoFromRow(ctx.NewRow)

	if ctx.DB != nil {
		enricher := NewPedidoEnricher(ctx.DB, ctx.Schema, ctx.Log)
		enricher.Enrich(&recurso)
	}

	tipoMod := TipoModificacaoModificado
	if ctx.Action == "INSERT" {
		tipoMod = TipoModificacaoCriado
	} else if ctx.Action == "DELETE" {
		tipoMod = TipoModificacaoDeletado
	}

	event := PedidoEvent{
		TipoModificacao: tipoMod,
		Recurso:         recurso,
	}

	return event, nil
}

func parsePedidoRecursoFromRow(row []interface{}) PedidoRecurso {
	rec := PedidoRecurso{}

	if len(row) > 0 {
		rec.ID = fmtValInt(row[0])
	}
	if len(row) > 1 {
		rec.Codigo = fmtValString(row[1])
	}
	if len(row) > 4 {
		rec.Status = fmtValInt(row[4])
	}
	if len(row) > 5 {
		rec.CodigoStatusErp = fmtValString(row[5])
	}
	if len(row) > 7 {
		rec.CodigoCondicaoPagamento = fmtValString(row[7])
	}
	if len(row) > 8 {
		rec.CodigoFormaPagamento = fmtValString(row[8])
	}
	if len(row) > 9 {
		rec.CodigoTipoVenda = fmtValString(row[9])
	}
	if len(row) > 11 {
		rec.CodigoTipoFrete = fmtValString(row[11])
	}
	if len(row) > 12 {
		rec.CodigoDeposito = fmtValString(row[12])
	}
	if len(row) > 13 {
		rec.CodigoEmpresa = fmtValString(row[13])
	}
	if len(row) > 15 {
		rec.CodigoEmpresaRepresentacao = fmtValString(row[15])
	}
	if len(row) > 16 {
		rec.CodigoTabelaPreco = fmtValString(row[16])
	}
	if len(row) > 17 {
		rec.CodigoTransportadora = fmtValString(row[17])
	}
	if len(row) > 19 {
		rec.CodigoRepresentante = fmtValString(row[19])
	}
	if len(row) > 20 {
		rec.CodigoLocalRedespacho = fmtValString(row[20])
	}
	if len(row) > 21 {
		rec.CodigoTransporte = fmtValString(row[21])
	}
	if len(row) > 22 {
		rec.CodigoCliente = fmtValString(row[22])
	}
	if len(row) > 24 {
		rec.AceitaFaturamentoParcial = fmtValInt(row[24])
	}
	if len(row) > 28 {
		rec.ProgramacaoItem = fmtValInt(row[28])
	}
	if len(row) > 29 {
		rec.ProntaEntrega = fmtValInt(row[29])
	}
	if len(row) > 40 {
		rec.Numero = fmtValInt(row[40])
	}
	if len(row) > 41 {
		rec.Comissao = fmtValFloat(row[41])
	}
	if len(row) > 42 {
		rec.Desconto = fmtValFloat(row[42])
	}
	if len(row) > 53 {
		rec.Portador = fmtValString(row[53])
	}
	if len(row) > 55 {
		rec.Referencia = fmtValString(row[55])
	}
	if len(row) > 56 {
		rec.Informacoes = fmtValString(row[56])
	}
	if len(row) > 57 {
		rec.ObservacaoCliente = fmtValString(row[57])
	}
	if len(row) > 58 {
		rec.DataBaseFaturamento = fmtValString(row[58])
	}
	if len(row) > 59 {
		rec.DataBaseOpcao = fmtValString(row[59])
	}
	if len(row) > 61 {
		rec.DataEmissao = fmtValString(row[61])
	}

	return rec
}

func fmtValString(val interface{}) string {
	if val == nil {
		return ""
	}
	if str, ok := val.(string); ok {
		return str
	}
	if b, ok := val.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", val)
}

func fmtValInt(val interface{}) int {
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	}
	return 0
}

func fmtValFloat(val interface{}) float64 {
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	case []byte:
		var f float64
		fmt.Sscanf(string(v), "%f", &f)
		return f
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return f
	}
	return 0
}

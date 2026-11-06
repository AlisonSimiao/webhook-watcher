package pedido

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

// Valores de PedidoRecurso.TipoEntrega — espelham constants.PEDIDO.ENTREGA no srp-hub-api.
const (
	entregaDireta     = 1
	entregaTriangular = 2
)

// Tipos de campos_personalizados.tipo — espelham constants.CAMPO_PERSONALIZADO.TIPO no srp-hub-api.
const (
	campoTipoTexto       = 1
	campoTipoNumerico    = 2
	campoTipoBoxMultipla = 3
	campoTipoBoxUnica    = 4
	campoTipoToggle      = 5
	campoTipoData        = 6
)

// Submódulos de campo personalizado do módulo Pedido (constants.CAMPO_PERSONALIZADO.SUBMODULO.PEDIDO)
// que chegam via cadastro_id = pedidos.id / tipo_cadastro = TIPO_CADASTRO.PEDIDO.
var camposPersonalizadosSubmodulos = map[int]string{
	1: "capaPedido",
	2: "informacoesAdicionais",
}

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
		// id_vendedor pode ser um Representante (tipo 2) ou um Usuário interno
		// (tipo 1) — ver getRepresentanteRepositoryAndText no srp-hub-api — por
		// isso a busca não filtra por tipo, só pelo id.
		queryUser := fmt.Sprintf("SELECT tenant, email, whatsapp, tipo FROM %s WHERE id = ? LIMIT 1", e.tableName("usuarios"))
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
		e.enrichDadosPedido(recurso)
		e.enrichEndereco(recurso)
		e.enrichCamposPersonalizados(recurso)
		e.enrichItens(recurso)
	}

	if recurso.CodigoCondicaoPagamento != "" {
		e.enrichParcelas(recurso)
	}
}

// enrichDadosPedido busca CNPJs (cliente, representada, empresa de representação,
// transportadora) e a data de programação de faturamento, todos por JOIN direto a
// partir do id do pedido — os ids (id_cliente, id_representada, id_empresa,
// id_transportadora) já chegaram em recurso.* via parsePedidoRecursoFromRow.
func (e *PedidoEnricher) enrichDadosPedido(recurso *PedidoRecurso) {
	query := fmt.Sprintf(`
		SELECT COALESCE(cli.cnpj_cpf, ''), 
		COALESCE(rep.cnpj_cpf, ''),
		COALESCE(er.cnpj, ''), 
		COALESCE(tra.cnpj_cpf, ''), 
		COALESCE(p.data_programacao, '')
		FROM %s p
		LEFT JOIN %s cli ON cli.id = p.id_cliente
		LEFT JOIN %s rep ON rep.id = p.id_representada
		LEFT JOIN %s er ON er.id = p.id_empresa
		LEFT JOIN %s tra ON tra.id = p.id_transportadora
		WHERE p.id = ?`,
		e.tableName("pedidos"),
		e.tableName("clientes"),
		e.tableName("representadas"),
		e.tableName("empresas_representantes"),
		e.tableName("transportadoras"))

	var cnpjCliente, cnpjEmpresa, cnpjEmpresaRepresentacao, cnpjTransportadora, dataProgramacao string
	err := e.db.QueryRow(query, recurso.ID).Scan(&cnpjCliente, &cnpjEmpresa, &cnpjEmpresaRepresentacao, &cnpjTransportadora, &dataProgramacao)
	if err != nil {
		e.log.Warn("Não foi possível buscar dados adicionais do pedido", "id_pedido", recurso.ID, "error", err)
		return
	}

	recurso.CnpjCpfCliente = cnpjCliente
	recurso.CnpjEmpresa = cnpjEmpresa
	recurso.CnpjCpfEmpresaRepresentacao = cnpjEmpresaRepresentacao
	recurso.CnpjCpfTransportadora = cnpjTransportadora
	recurso.DataPrevisaoFaturamento = dataProgramacao
}

// enrichEndereco busca o endereço de entrega do pedido (pedidos_enderecos), com o
// nome do IBGE da cidade e, quando a entrega é triangular (id_triangular != NULL),
// os dados da empresa/local de triangulação.
func (e *PedidoEnricher) enrichEndereco(recurso *PedidoRecurso) {
	query := fmt.Sprintf(`SELECT COALESCE(pe.cep, ''), COALESCE(pe.endereco, ''), COALESCE(pe.complemento, ''),
		COALESCE(pe.numero, ''), COALESCE(pe.bairro, ''), COALESCE(ci.codigo_ibge, ''),
		COALESCE(tr.codigo, ''), COALESCE(tr.cnpj, ''), pe.id_triangular
		FROM %s pe
		LEFT JOIN %s ci ON ci.id = pe.id_cidade
		LEFT JOIN %s tr ON tr.id = pe.id_triangular
		WHERE pe.id_pedido = ? LIMIT 1`,
		e.tableName("pedidos_enderecos"),
		e.tableName("cidades"),
		e.tableName("triangulares"))

	var cep, endereco, complemento, numero, bairro, codigoCidade, codigoTriangular, cnpjTriangular string
	var idTriangular sql.NullInt64
	err := e.db.QueryRow(query, recurso.ID).Scan(&cep, &endereco, &complemento, &numero, &bairro, &codigoCidade, &codigoTriangular, &cnpjTriangular, &idTriangular)
	if err != nil {
		if err != sql.ErrNoRows {
			e.log.Warn("Não foi possível buscar endereço de entrega do pedido", "id_pedido", recurso.ID, "error", err)
		}
		return
	}

	recurso.Cep = cep
	recurso.Endereco = endereco
	recurso.Complemento = complemento
	recurso.EnderecoNumero = numero
	recurso.Bairro = bairro
	recurso.CodigoCidade = codigoCidade

	if idTriangular.Valid {
		recurso.TipoEntrega = entregaTriangular
		recurso.CodigoTriangular = codigoTriangular
		recurso.CnpjCpfTriangular = cnpjTriangular
	} else {
		recurso.TipoEntrega = entregaDireta
	}
}

// enrichParcelas busca os dias de vencimento de cada parcela da condição de
// pagamento do pedido — não há coluna própria, o array é reconstituído a partir
// das linhas filhas de condicoes_pagamento_parcelas.
func (e *PedidoEnricher) enrichParcelas(recurso *PedidoRecurso) {
	query := fmt.Sprintf(`SELECT dias FROM %s WHERE id_condicao = ? ORDER BY id`, e.tableName("condicoes_pagamento_parcelas"))
	rows, err := e.db.Query(query, recurso.CodigoCondicaoPagamento)
	if err != nil {
		e.log.Warn("Não foi possível buscar parcelas do pedido", "id_condicao_pgto", recurso.CodigoCondicaoPagamento, "error", err)
		return
	}
	defer rows.Close()

	parcelas := []int{}
	for rows.Next() {
		var dias int
		if errScan := rows.Scan(&dias); errScan != nil {
			e.log.Warn("Não foi possível escanear parcela do pedido", "id_condicao_pgto", recurso.CodigoCondicaoPagamento, "error", errScan)
			continue
		}
		parcelas = append(parcelas, dias)
	}
	if err := rows.Err(); err != nil {
		e.log.Warn("Erro ao iterar parcelas do pedido", "id_condicao_pgto", recurso.CodigoCondicaoPagamento, "error", err)
		return
	}
	recurso.Parcelas = parcelas
}

// enrichCamposPersonalizados busca as respostas de campos personalizados do
// pedido (tipo_cadastro = Pedido), agrupadas por submódulo (capaPedido /
// informacoesAdicionais), formatando o valor de acordo com o tipo do campo —
// espelha CampoPersonalizadoRespostaService.buildResponse/getFieldAnswer do
// srp-hub-api.
func (e *PedidoEnricher) enrichCamposPersonalizados(recurso *PedidoRecurso) {
	const tipoCadastroPedido = 1

	query := fmt.Sprintf(`SELECT cp.submodulo_campo_personalizado_id, cp.titulo, cp.tipo,
		COALESCE(cpr.valor, ''), COALESCE(cpr.opcoes_ids, '')
		FROM %s cpr
		JOIN %s cp ON cp.id = cpr.campo_personalizado_id
		WHERE cpr.tipo_cadastro = ? AND cpr.cadastro_id = ?`,
		e.tableName("campos_personalizados_respostas"),
		e.tableName("campos_personalizados"))

	rows, err := e.db.Query(query, tipoCadastroPedido, recurso.ID)
	if err != nil {
		e.log.Warn("Não foi possível buscar campos personalizados do pedido", "id_pedido", recurso.ID, "error", err)
		return
	}
	defer rows.Close()

	camposPersonalizados := map[string]map[string]interface{}{}
	for rows.Next() {
		var idSubmodulo, tipo int
		var titulo, valor, opcoesIDs string
		if errScan := rows.Scan(&idSubmodulo, &titulo, &tipo, &valor, &opcoesIDs); errScan != nil {
			e.log.Warn("Não foi possível escanear campo personalizado do pedido", "id_pedido", recurso.ID, "error", errScan)
			continue
		}

		submodulo, ok := camposPersonalizadosSubmodulos[idSubmodulo]
		if !ok {
			continue
		}

		var titulos []string
		if tipo == campoTipoBoxMultipla || tipo == campoTipoBoxUnica {
			titulos = e.resolveOpcaoTitulos(parseOpcoesIDs(opcoesIDs))
		}

		if camposPersonalizados[submodulo] == nil {
			camposPersonalizados[submodulo] = map[string]interface{}{}
		}
		camposPersonalizados[submodulo][titulo] = formatCampoPersonalizadoValor(tipo, valor, titulos)
	}
	if err := rows.Err(); err != nil {
		e.log.Warn("Erro ao iterar campos personalizados do pedido", "id_pedido", recurso.ID, "error", err)
		return
	}

	recurso.CamposPersonalizados = camposPersonalizados
}

// resolveOpcaoTitulos traduz ids de campos_personalizados_opcoes para seus
// títulos, preservando a ordem de ids recebida.
func (e *PedidoEnricher) resolveOpcaoTitulos(ids []int64) []string {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf("SELECT id, titulo FROM %s WHERE id IN (%s)", e.tableName("campos_personalizados_opcoes"), strings.Join(placeholders, ", "))
	rows, err := e.db.Query(query, args...)
	if err != nil {
		e.log.Warn("Não foi possível buscar opções de campo personalizado", "ids", ids, "error", err)
		return nil
	}
	defer rows.Close()

	titulosPorID := make(map[int64]string, len(ids))
	for rows.Next() {
		var id int64
		var titulo string
		if errScan := rows.Scan(&id, &titulo); errScan != nil {
			e.log.Warn("Não foi possível escanear opção de campo personalizado", "error", errScan)
			continue
		}
		titulosPorID[id] = titulo
	}
	if err := rows.Err(); err != nil {
		e.log.Warn("Erro ao iterar opções de campo personalizado", "ids", ids, "error", err)
		return nil
	}

	titulos := make([]string, 0, len(ids))
	for _, id := range ids {
		if titulo, ok := titulosPorID[id]; ok {
			titulos = append(titulos, titulo)
		}
	}
	return titulos
}

func (e *PedidoEnricher) enrichItens(recurso *PedidoRecurso) {
	// Colunas de pedidos_produtos com COALESCE para NULLs. codigoEmpresa e
	// codigoCor vêm de JOINs (representadas e produtos_opcoes); codigoUnidadeMedida
	// vem do produto do item (produtos_representadas -> unidades_medidas); os
	// demais campos são colunas diretas da tabela de itens.
	queryItens := fmt.Sprintf(`SELECT COALESCE(pp.sequencia, 0), COALESCE(pp.codigo, ''),
		COALESCE(r.codigo, ''), COALESCE(po.codigo, ''), COALESCE(um.abreviacao, ''),
		COALESCE(pp.codigo_integracao, ''), COALESCE(pp.quantidade, 0),
		COALESCE(pp.preco, 0), COALESCE(pp.preco_original, 0),
		COALESCE(pp.faturado, 0), COALESCE(pp.cancelado, 0), COALESCE(pp.peso, 0),
		COALESCE(pp.desconto, 0), COALESCE(pp.icms_valor, 0), COALESCE(pp.ipi_valor, 0),
		COALESCE(pp.informacoes, ''), pp.data_emissao, COALESCE(pp.previsao_faturamento, '')
		FROM %s pp
		LEFT JOIN %s r ON r.id = pp.id_representada
		LEFT JOIN %s po ON po.id = pp.id_opcao_item
		LEFT JOIN %s prod ON prod.id = pp.id_produtos_representada
		LEFT JOIN %s um ON um.id = prod.id_unidade_medida
		WHERE pp.id_pedido = ?`,
		e.tableName("pedidos_produtos"),
		e.tableName("representadas"),
		e.tableName("produtos_opcoes"),
		e.tableName("produtos_representadas"),
		e.tableName("unidades_medidas"))
	rows, err := e.db.Query(queryItens, recurso.ID)
	if err != nil {
		e.log.Warn("Não foi possível buscar itens do pedido", "id_pedido", recurso.ID, "error", err)
		return
	}
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

// parseOpcoesIDs faz parse de campos_personalizados_respostas.opcoes_ids, gravado
// como um literal de array sem espaços (ex: "[1,2]"), para uma lista de ids.
func parseOpcoesIDs(raw string) []int64 {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "[")
	raw = strings.TrimSuffix(raw, "]")
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	ids := make([]int64, 0, len(parts))
	for _, part := range parts {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// formatCampoPersonalizadoValor espelha CampoPersonalizadoRespostaService.getFieldAnswer
// do srp-hub-api: o formato do valor depende do tipo do campo personalizado.
func formatCampoPersonalizadoValor(tipo int, valor string, titulos []string) interface{} {
	switch tipo {
	case campoTipoNumerico:
		return parseNumeroBR(valor)
	case campoTipoToggle:
		n, _ := strconv.Atoi(valor)
		return n
	case campoTipoBoxMultipla:
		if titulos == nil {
			return []string{}
		}
		return titulos
	case campoTipoBoxUnica:
		if len(titulos) == 0 {
			return ""
		}
		return titulos[0]
	default: // texto, data
		return valor
	}
}

// parseNumeroBR faz parse de um número no formato brasileiro (ex: "1.234,56"),
// como armazenado em campos_personalizados_respostas.valor para campos numéricos.
func parseNumeroBR(valor string) float64 {
	if valor == "" {
		return 0
	}
	normalized := strings.ReplaceAll(valor, ".", "")
	normalized = strings.ReplaceAll(normalized, ",", ".")
	n, _ := strconv.ParseFloat(normalized, 64)
	return n
}

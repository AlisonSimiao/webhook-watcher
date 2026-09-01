package pedido

type TipoModificacao string

const (
	TipoModificacaoCriado     TipoModificacao = "C"
	TipoModificacaoModificado TipoModificacao = "M"
	TipoModificacaoDeletado   TipoModificacao = "D"
)

type PedidoItem struct {
	Sequencia               int     `json:"sequencia"`
	CodigoProduto           string  `json:"codigoProduto"`
	CodigoEmpresa           string  `json:"codigoEmpresa"`
	CodigoCor               string  `json:"codigoCor"`
	CodigoUnidadeMedida     string  `json:"codigoUnidadeMedida"`
	DataEmissao             string  `json:"dataEmissao"`
	DataPrevisaoFaturamento string  `json:"dataPrevisaoFaturamento"`
	Faturado                float64 `json:"faturado"`
	Quantidade              float64 `json:"quantidade"`
	Cancelado               float64 `json:"cancelado"`
	Peso                    float64 `json:"peso"`
	Desconto                float64 `json:"desconto"`
	PrecoBruto              float64 `json:"precoBruto"`
	PrecoLiquido            float64 `json:"precoLiquido"`
	Icms                    float64 `json:"icms"`
	Ipi                     float64 `json:"ipi"`
	Informacoes             string  `json:"informacoes"`
	CodigoIntegracao        string  `json:"codigoIntegracao"`
}

type PedidoRecurso struct {
	Tenant                      string                            `json:"tenant"`
	ID                          int                               `json:"id"`
	Codigo                      string                            `json:"codigo"`
	Numero                      int                               `json:"numero"`
	CodigoDeposito              string                            `json:"codigoDeposito"`
	CodigoCliente               string                            `json:"codigoCliente"`
	CnpjCpfCliente              string                            `json:"cnpjCpfCliente"`
	CodigoEmpresaRepresentacao  string                            `json:"codigoEmpresaRepresentacao"`
	CnpjCpfEmpresaRepresentacao string                            `json:"cnpjCpfEmpresaRepresentacao"`
	CodigoRepresentante         string                            `json:"codigoRepresentante"`
	EmailRepresentante          string                            `json:"emailRepresentante"`
	WhatsappRepresentante       string                            `json:"whatsappRepresentante"`
	TipoRepresentante           int                               `json:"tipoRepresentante"`
	CodigoTipoVenda             string                            `json:"codigoTipoVenda"`
	CodigoStatusErp             string                            `json:"codigoStatusErp"`
	Parcelas                    []int                             `json:"parcelas"`
	CodigoCondicaoPagamento     string                            `json:"codigoCondicaoPagamento"`
	CodigoTabelaPreco           string                            `json:"codigoTabelaPreco"`
	CodigoLocalRedespacho       string                            `json:"codigoLocalRedespacho"`
	CodigoTransporte            string                            `json:"codigoTransporte"`
	CodigoTransportadora        string                            `json:"codigoTransportadora"`
	CnpjCpfTransportadora       string                            `json:"cnpjCpfTransportadora"`
	CodigoFormaPagamento        string                            `json:"codigoFormaPagamento"`
	CodigoTipoFrete             string                            `json:"codigoTipoFrete"`
	CodigoTriangular            string                            `json:"codigoTriangular"`
	CnpjCpfTriangular           string                            `json:"cnpjCpfTriangular"`
	Referencia                  string                            `json:"referencia"`
	Informacoes                 string                            `json:"informacoes"`
	ObservacaoCliente           string                            `json:"observacaoCliente"`
	Portador                    string                            `json:"portador"`
	Comissao                    float64                           `json:"comissao"`
	Desconto                    float64                           `json:"desconto"`
	AceitaFaturamentoParcial    int                               `json:"aceitaFaturamentoParcial"`
	TipoEntrega                 int                               `json:"tipoEntrega"`
	Cep                         string                            `json:"cep"`
	Endereco                    string                            `json:"endereco"`
	EnderecoNumero              string                            `json:"enderecoNumero"`
	Complemento                 string                            `json:"complemento"`
	Bairro                      string                            `json:"bairro"`
	CodigoCidade                string                            `json:"codigoCidade"`
	ProntaEntrega               int                               `json:"prontaEntrega"`
	ProgramacaoItem             int                               `json:"programacaoItem"`
	DataPrevisaoFaturamento     string                            `json:"dataPrevisaoFaturamento"`
	DataEmissao                 string                            `json:"dataEmissao"`
	DataBaseFaturamento         string                            `json:"dataBaseFaturamento"`
	DataBaseOpcao               string                            `json:"dataBaseOpcao"`
	Status                      int                               `json:"status"`
	CodigoEmpresa               string                            `json:"codigoEmpresa"`
	CnpjEmpresa                 string                            `json:"cnpjEmpresa"`
	CamposPersonalizados        map[string]map[string]interface{} `json:"camposPersonalizados"`
	Itens                       []PedidoItem                      `json:"itens"`
}

type PedidoEvent struct {
	TipoModificacao TipoModificacao `json:"tipoModificacao"`
	Recurso         PedidoRecurso   `json:"recurso"`
}

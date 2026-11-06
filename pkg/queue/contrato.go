package queue

import "encoding/json"

// TipoModificacao classifica a mudança que originou o Evento.
type TipoModificacao string

const (
	TipoModificacaoCriado     TipoModificacao = "C"
	TipoModificacaoModificado TipoModificacao = "M"
	TipoModificacaoDeletado   TipoModificacao = "D"
)

// TipoModificacaoDe traduz a ação do binlog para o tipo de modificação do
// contrato.
func TipoModificacaoDe(action Action) TipoModificacao {
	switch action {
	case ActionInsert:
		return TipoModificacaoCriado
	case ActionDelete:
		return TipoModificacaoDeletado
	default:
		return TipoModificacaoModificado
	}
}

// Contrato é o formato canônico do Evento (docs/adr/0003), carregado em
// Event.Payload. É definido por este projeto, não por quem consome: cada tipo
// de consumer traduz o Contrato para o formato do seu destino.
//
// O Evento é um snapshot do Recurso (docs/adr/0001): DataEvento é o instante
// em que o estado foi lido (ISO-8601 UTC com milissegundos), e quem consome
// descarta um snapshot mais antigo que o último recebido do mesmo Recurso.
type Contrato struct {
	IDEvento        string          `json:"idEvento"`
	Tipo            string          `json:"tipo"`
	TipoModificacao TipoModificacao `json:"tipoModificacao"`
	DataEvento      string          `json:"dataEvento"`
	Tenant          string          `json:"tenant"`
	Recurso         json.RawMessage `json:"recurso"`
}

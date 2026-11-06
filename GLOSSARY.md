# Webhook Watcher

Captura mudanças nas tabelas do ERP (via binlog do MariaDB) e as publica como eventos para que os clientes do SRP Hub sejam notificados.

## Language

**Evento**:
Um snapshot do estado atual de um Recurso, emitido porque o Recurso mudou. Um Evento mais novo do mesmo Recurso substitui o anterior; não é um fato de negócio isolado ("PedidoFaturado") nem fonte da verdade.
_Avoid_: mensagem, task, hook, evento de domínio

**Data do evento** (`dataEvento`):
O instante em que o estado do Recurso contido no Evento foi lido. Entre dois Eventos do mesmo Recurso, o de data maior é o mais novo.
_Avoid_: createdAt, timestamp, data de criação (essa é do Recurso)

**Recurso**:
A entidade de negócio de um tenant que um Evento descreve por completo (hoje, o pedido com seus itens), identificada por tenant + tabela + id.
_Avoid_: registro, linha, row

**Tipo de modificação**:
Classificação da mudança que originou o Evento: `C` (criado), `M` (modificado) ou `D` (deletado).
_Avoid_: action, operação

**Tenant**:
O schema do ERP ao qual um Recurso pertence; cada tenant é um cliente do SRP Hub.
_Avoid_: schema, banco, empresa

**Hook**:
Destino registrado por um cliente para receber Eventos de um tipo de Recurso. Resolvido pelo srp-hub-api, não por este projeto.
_Avoid_: webhook (para o Evento em si), callback

**Tipo de consumer**:
Uma forma de levar Eventos para fora (hoje só HTTP, repassando ao srp-hub-api); cada tipo recebe sua própria cópia de cada Evento.
_Avoid_: subscriber, worker

**Entrega**:
A tentativa de levar um Evento a um tipo de consumer. É pelo menos uma vez: o mesmo Evento pode ser entregue mais de uma vez, e nem sempre na ordem em que foi emitido.
_Avoid_: envio, dispatch

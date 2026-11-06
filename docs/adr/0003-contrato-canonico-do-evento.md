# Contrato canônico do Evento, independente de consumer

O formato do Evento é definido por este projeto e preenchido pelo producer de forma genérica para qualquer Recurso; cada tipo de consumer traduz esse contrato para o formato do seu destino. O srp-hub-api é só um dos consumidores possíveis (SSE, notificações e outros destinos HTTP são previstos), então o contrato não espelha o `hook.dto.js` de lá — mesmo que hoje a tradução para ele seja quase a identidade. Metadados do Evento (como o instante em que o estado do Recurso foi lido, que permite a quem consome descartar snapshots antigos — ver ADR 0001) ficam ao lado de `tipoModificacao`, não dentro do `recurso`.

## Consequences

- O consumer HTTP envia ao srp-hub-api o contrato canônico inteiro como corpo, sem tradução: metadados como `idEvento` e `dataEvento` vão mesmo para um destino que ainda não os usa, porque outros consumidores podem usá-los e um campo novo no contrato chega sem mexer no consumer. A rota `/interno/hooks/<tipo>` aceita o corpo (valida só `tipoModificacao` e `recurso`).
- O srp-hub-api não é alterado por esta decisão: ele monta o que entrega ao cliente final só com `tipoModificacao` e `recurso` (`HookSendDto`), então os clientes atendidos por ele ainda não recebem os metadados (e não podem descartar snapshots antigos) até que aquele lado passe a repassá-los.
- Antes, o adapter traduzia o contrato de volta para exatamente `{tipoModificacao, recurso}`; isso foi revertido (2026-11-06) para não esconder os metadados de quem está do outro lado.

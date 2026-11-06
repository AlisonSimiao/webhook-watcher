# Contrato canônico do Evento, independente de consumer

O formato do Evento é definido por este projeto e preenchido pelo producer de forma genérica para qualquer Recurso; cada tipo de consumer traduz esse contrato para o formato do seu destino. O srp-hub-api é só um dos consumidores possíveis (SSE, notificações e outros destinos HTTP são previstos), então o contrato não espelha o `hook.dto.js` de lá — mesmo que hoje a tradução para ele seja quase a identidade. Metadados do Evento (como o instante em que o estado do Recurso foi lido, que permite a quem consome descartar snapshots antigos — ver ADR 0001) ficam ao lado de `tipoModificacao`, não dentro do `recurso`.

## Consequences

- O adapter do srp-hub-api traduz o contrato canônico de volta para exatamente `{tipoModificacao, recurso}` — o que aquela rota recebe hoje. Por decisão, mudanças no contrato não exigem nem provocam mudanças no srp-hub-api; por isso, por enquanto, os clientes atendidos por ele não recebem os metadados novos (e não podem descartar snapshots antigos).

# Fila de tarefas (asynq) com fan-out no producer, não um log de eventos

O transporte é o asynq — uma fila de tarefas com competing consumers — e o producer publica uma cópia de cada Evento por tipo de consumer (`FanoutQueue` em `initQueue`). Isso significa que o producer conhece os tipos de consumer (um novo tipo exige reimplantar o watcher) e que não há retenção nem replay: um consumer novo não lê o histórico. Aceitamos isso porque só existe um tipo de consumer (HTTP) e o asynq já dá retry, backoff e arquivamento prontos; trocar para um log com consumer groups (Redis Streams, Kafka) é a saída quando um segundo tipo de consumer for real ou quando replay for necessário.

## Considered Options

- **Redis Streams com consumer groups**: assinatura independente por tipo de consumer e retenção, no mesmo Redis — mas retry, backoff, dead-letter e ordenação por recurso (particionamento) teriam de ser implementados à mão.
- **Kafka**: log particionado resolve ordem por chave e replay, ao custo de uma infraestrutura nova para um único consumer.

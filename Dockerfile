# BUILDER da aplicação em go
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o webhook-watcher .

# APLICAÇÃO FINAL
FROM alpine:3.23

WORKDIR /app

COPY --from=builder /app/webhook-watcher .

# servers.db (servidores, posição do binlog, dead-letters) fica em /data —
# monte um volume persistente aqui, senão cada restart perde a posição e
# volta para o SHOW MASTER STATUS. Producer e consumer devem montar o MESMO
# volume para compartilhar o arquivo.
RUN mkdir -p /data
ENV SQLITE_PATH=/data/servers.db
VOLUME ["/data"]

# Sem argumentos roda o producer (watcher do binlog); o consumer roda com
# "consumer http" (ex: command: ["consumer", "http"] no compose).
ENTRYPOINT ["./webhook-watcher"]

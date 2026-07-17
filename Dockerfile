FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY src/go.mod src/go.sum ./
RUN go mod download

COPY src/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /ingestao main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /ingestao /app/ingestao

CMD ["/app/ingestao"]

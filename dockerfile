from golang:1.26-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o mytruyen-worker main.go

from alpine:latest

workdir /app

copy --from=builder /app/mytruyen-worker .

CMD ["./mytruyen-worker"]
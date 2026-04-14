# syntax=docker/dockerfile:1.7

FROM golang:1.25.3-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -trimpath -ldflags="-s -w" -o /out/mytruyen-worker ./main.go

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata
RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

COPY --from=builder /out/mytruyen-worker /usr/local/bin/mytruyen-worker

USER app

ENTRYPOINT ["/usr/local/bin/mytruyen-worker"]

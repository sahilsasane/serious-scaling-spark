FROM golang:1.24-bookworm AS builder
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN go build -o lab .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    strace \
    lsof \
    procps \
    linux-perf \
    curl \
    htop \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/lab ./lab
COPY scripts/ ./scripts/

EXPOSE 8080
CMD ["./lab"]

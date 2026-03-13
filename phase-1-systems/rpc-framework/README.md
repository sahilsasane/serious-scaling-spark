# Custom RPC Framework (Go)

Minimal RPC framework using TCP + JSON with:
- Method dispatch on server
- Request/response envelope with request IDs
- Client-side round-robin load balancing
- Per-attempt timeout and overall context support
- Retries for retryable transport/deadline errors
- Exponential backoff with jitter

## Run

```bash
go run .
```

This starts two servers (`:9001`, `:9002`) and performs sample client calls to `math.add`.

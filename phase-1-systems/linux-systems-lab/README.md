# Linux Systems Lab (Week 7-8)

A simple service designed to create CPU, memory, and I/O pressure so you can practice Linux profiling tools.

## Endpoints
- `GET /health`
- `GET /cpu?loops=3000000`
- `GET /io?mb=32`
- `GET /mem?mb=128`
- `GET /mixed?workers=4&loops=1000000`

## Run (Docker — recommended, gives you real Linux perf/strace)
```bash
# start the service
docker compose up -d lab

# open a tools shell (perf, strace, lsof, htop all available)
docker compose run --rm tools
```

Inside the tools shell:
```bash
# hit the service
curl http://localhost:8080/cpu
bash scripts/load.sh http://localhost:8080 20
```

## Run (local macOS)
```bash
go run .
```

## Generate load
```bash
chmod +x scripts/load.sh
./scripts/load.sh http://localhost:8080 20
```

## Profiling checklist (Linux)
1. Find pid:
```bash
pgrep -f linuxsystemslab
```
2. Syscall profile:
```bash
strace -c -p <pid>
```
3. Open files/sockets:
```bash
lsof -p <pid>
```
4. CPU hot paths:
```bash
perf top -p <pid>
```
5. Scheduler + CPU view:
```bash
top -H -p <pid>
```
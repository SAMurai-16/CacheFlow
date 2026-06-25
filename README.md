# CacheFlow


A small Redis-compatible server written in Go. It speaks RESP over TCP, stores data in memory, and implements a focused subset of Redis commands across strings, lists, streams, pub/sub, transactions, persistence loading, and basic replication.

This project is useful for learning how Redis works internally: request parsing, command dispatch, key expiry, stream IDs, replication handshakes, and RESP encoding are all implemented directly in the codebase.

## Features

- RESP array parsing for Redis CLI compatible requests
- TCP server with concurrent client handling
- String commands: `PING`, `ECHO`, `SET`, `GET`, `INCR`
- Key commands: `TYPE`, `KEYS`
- List commands: `LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`
- Stream commands: `XADD`, `XRANGE`, `XREAD`
- Pub/sub commands: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- Transactions: `MULTI`, `EXEC`, `DISCARD`
- Replication commands: `INFO`, `REPLCONF`, `PSYNC`, `WAIT`
- RDB loading for string keys from a Redis RDB file
- Config lookup for `dir` and `dbfilename`

## Requirements

- Go 1.25.6 or newer, matching `go.mod`
- Optional: `redis-cli` for manual testing

## Build

```bash
go build -o go_redis_server ./cmd/server
```

## Run

Start a standalone server:

```bash
./go_redis_server --port 6380
```

The server listens on port `6380` by default if `--port` is omitted.

Connect with Redis CLI:

```bash
redis-cli -p 6380
```

Example session:

```bash
redis-cli -p 6380 SET greeting hello
redis-cli -p 6380 GET greeting
redis-cli -p 6380 INCR counter
redis-cli -p 6380 TYPE greeting
```

## Benchmarks

Local benchmark using `redis-benchmark` with 100,000 requests, 50 parallel
clients, 3-byte payloads, keep-alive enabled, and AOF disabled for Redis.

Benchmark command:

```bash
redis-benchmark -h 127.0.0.1 -p <port> -t set,get,incr -n 100000 -c 50
```

In this run, CacheFlow was running on port `6380` and Redis was running on
port `6381`.

| Command | CacheFlow req/s | Redis req/s | CacheFlow avg latency | Redis avg latency | CacheFlow p99 | Redis p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `SET` | 91,743.12 | 145,560.41 | 0.359 ms | 0.185 ms | 1.239 ms | 0.551 ms |
| `GET` | 87,183.96 | 135,685.22 | 0.374 ms | 0.200 ms | 1.183 ms | 0.639 ms |
| `INCR` | 91,659.03 | 136,798.91 | 0.356 ms | 0.198 ms | 1.255 ms | 0.639 ms |

### Concurrency Scaling

CacheFlow-only benchmark using 100,000 requests, 3-byte payloads, keep-alive
enabled, and varying client concurrency.

```bash
redis-benchmark -p 6380 -t set,get,incr -n 100000 -c <clients> -q
```

| Clients | SET req/s | SET p50 | GET req/s | GET p50 | INCR req/s | INCR p50 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 20,876.83 | 0.039 ms | 20,876.83 | 0.039 ms | 10,801.47 | 0.047 ms |
| 10 | 50,761.42 | 0.135 ms | 32,258.07 | 0.135 ms | 50,175.61 | 0.143 ms |
| 50 | 101,419.88 | 0.271 ms | 104,602.52 | 0.263 ms | 103,734.44 | 0.271 ms |
| 100 | 102,880.66 | 0.503 ms | 105,708.25 | 0.503 ms | 102,669.41 | 0.519 ms |
| 200 | 109,890.11 | 0.903 ms | 109,890.11 | 0.879 ms | 108,932.46 | 0.919 ms |

These numbers are local microbenchmarks, not a full production performance
claim. CacheFlow implements a smaller Redis-compatible surface area, while
Redis provides much broader command semantics, persistence, memory management,
replication behavior, and operational features.

## Startup Options

```bash
./go_redis_server [--port <port>] [--replicaof <host> <port>] [--dir <path>] [--dbfilename <file>]
```

Options:

- `--port <port>`: TCP port to listen on. Defaults to `6380`.
- `--replicaof <host> <port>`: start this server as a replica of the given master.
- `--dir <path>`: directory used when loading an RDB file. Defaults to `.`.
- `--dbfilename <file>`: RDB filename to load. Defaults to `dump.rdb`.

## Persistence

On startup, the server attempts to load an RDB file from:

```text
<dir>/<dbfilename>
```

For example:

```bash
./go_redis_server --port 6380 --dir /tmp/redis-data --dbfilename dump.rdb
```

The current RDB loader supports Redis RDB string values, including keys with second or millisecond expiry metadata.

## Replication

Start a master:

```bash
./go_redis_server --port 6380
```

Start a replica:

```bash
./go_redis_server --port 6381 --replicaof 127.0.0.1 6380
```

Try a replicated write:

```bash
redis-cli -p 6380 SET mykey hello
redis-cli -p 6381 GET mykey
```

Check roles:

```bash
redis-cli -p 6380 INFO replication
redis-cli -p 6381 INFO replication
```

Replication is intentionally minimal. The server performs the `PING`, `REPLCONF`, and `PSYNC` handshake, sends an empty RDB snapshot for full sync, and streams propagated write commands from master to replica. The replica apply path currently handles `SET`; other propagated write commands are still limited.

See [replication.md](replication.md) for a more detailed walkthrough and manual test plan.

## Project Structure

```text
cmd/server/       TCP server entrypoint and connection loop
commands/         Redis command handlers
engine/           Command dispatcher
helper/           Replication connection and propagation helpers
resp/             RESP parser
store/            In-memory data store, streams, RDB loading, counters
replication.md    Replication notes and manual test steps
```

## Development

Run all Go package checks:

```bash
go test ./...
```

Format code:

```bash
go fmt ./...
```

## Notes and Limitations

- This is a learning implementation, not a production Redis replacement.
- Data is stored in memory; RDB support currently loads data on startup but does not save snapshots.
- Replication support is partial and primarily demonstrates the full-sync handshake and command propagation.
- Pub/sub subscriptions are connection-local and managed in memory.
- Stream reads are non-blocking.

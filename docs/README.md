# db-cluster — Raft Consensus Engine with HA Failover

TypeScript distributed consensus layer built on [@maboke123/raft-core](https://www.npmjs.com/package/@maboke123/raft-core) with mTLS transport, SQLite storage, sharding, and high-availability failover.

## Why This Exists

Pure-TS replacement for WASM-based Raft implementations. No WASM serialization queues, no RefCell contention, no manual tick driving — `RaftNode` internally manages timers and elections.

## Architecture

See [architecture.md](./architecture.md) for:
- Component diagram (29 modules across 6 layers)
- Data flow: propose path, receive path, heartbeat, connection lifecycle
- Error state machine with propagation paths
- Wire format: 4-byte length prefix + msgpack payload
- All design decisions D1–D9 with rationale

## Project Stats

| Metric | Value |
|--------|-------|
| Source files | ~30 |
| Source lines | 13,684 |
| Test files | 43 |
| Test lines | 10,379 |
| Tests | 391 |
| [TypeScript](https://www.typescriptlang.org/) errors | 0 |
| [ESLint](https://eslint.org/) errors | 0 |
| [Prettier](https://prettier.io/) compliance | 100% |
| Dead code (knip) | 0 |

## Quick Start

```bash
# Install
npm install

# Full test suite (~50s)
npm test

# Lint
npm run lint

# Format check
npm run format

# Dead code scan
npm run dead

# Build
npm run build
```

## Test Suites

| Category | Directory | Files | Description |
|----------|-----------|-------|-------------|
| Unit | `src/__tests__/unit/` | 26 | Pure function + mock-based, no network |
| Integration | `src/__tests__/integration/` | 5 | Real 3-node mTLS clusters in sub-process |
| Performance | `src/__tests__/performance/` | 3 | Throughput, election, chaos engineering |
| Interface | `src/__tests__/interfaces/` | 12 | Type-level contract verification |

### Run by Category

```bash
npx vitest run src/__tests__/unit/         # 26 files, ~30s
npx vitest run src/__tests__/integration/   # 5 files, ~50s  (real mTLS clusters)
npx vitest run src/__tests__/performance/   # 3 files, ~20s
npx vitest run src/__tests__/interfaces/    # 12 files, <1s

# Single file
npx vitest run src/__tests__/integration/ha-e2e.test.ts

# With Raft debug logs
DEBUG=1 npx vitest run src/__tests__/performance/raft-chaos.test.ts
```

## Core Modules

| Module | Lines | Role |
|--------|-------|------|
| `RaftGroupManager` | 1,921 | Cluster lifecycle: start/stop/configure all Raft groups |
| `RaftGroup` | 1,782 | Per-shard Raft group: propose, commit, catch-up, snapshot |
| `ConnectionPool` | 1,176 | mTLS connection management: connect, retry, backoff, circuit breaker |
| `SqliteStorage` | 660 | Raft log + state machine storage (SQLite WAL mode) |
| `SharedBatchedTransport` | 907 | Batched message transport with shared port mux |
| `HAFailoverCoordinator` | 762 | Automatic leader election and failover orchestration |
| `RaftAdapter` | 774 | @maboke123/raft-core wrapper — normalizes API surface |

## Testing Infrastructure

### In-Process mTLS Clusters

Integration and performance tests spin up real 3-node Raft clusters with TLS certificates inside the same process (not external processes). No Docker, no network config:

```
┌─────────────────────────────────────────────────┐
│                   vitest fork                    │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ Node 1   │  │ Node 2   │  │ Node 3   │      │
│  │ :19001   │  │ :19002   │  │ :19003   │      │
│  │ RaftNode │  │ RaftNode │  │ RaftNode │      │
│  │ RaftTcp  │  │ RaftTcp  │  │ RaftTcp  │      │
│  │ Sqlite   │  │ Sqlite   │  │ Sqlite   │      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘      │
│       └─────────────┼──────────────┘             │
│                     │                            │
│              mTLS (self-signed)                  │
└─────────────────────────────────────────────────┘
```

### Chaos Engineering

The `raft-chaos.test.ts` suite verifies Raft safety invariants under controlled failures:

- **Leader kill**: New election completes with no data loss
- **Follower kill**: Quorum maintained, leader commits new entries
- **Leader flapping**: 3 consecutive leader kills — term progression monotonic

## Design Decisions

1. **`pool: forks`** — All tests run in isolated processes; prevents module cache contamination from `vi.mock()`
2. **No `vi.useFakeTimers`** — Raft's `setInterval` health checks hang indefinitely with fake timers
3. **Pure-function extraction** — `encodeFrame`/`decodeFrame` tested separately from TLS-dependent `PeerConnection`
4. **`TestCluster`** — Shared 3-node cluster startup in `src/__tests__/integration/TestCluster.ts`
5. **`getErrorMessage(err: unknown)`** — All catch blocks use type-safe error extraction instead of `catch (err: any)`

## Dependencies

- [`@maboke123/raft-core`](https://www.npmjs.com/package/@maboke123/raft-core) — Pure-TS Raft consensus engine
- [`@msgpack/msgpack`](https://www.npmjs.com/package/@msgpack/msgpack) — MessagePack binary serialization
- [`better-sqlite3`](https://www.npmjs.com/package/better-sqlite3) — SQLite3 bindings (WAL mode)
- [`pino`](https://www.npmjs.com/package/pino) — Structured logging
- [`opossum`](https://www.npmjs.com/package/opossum) — Circuit breaker

## License

MIT

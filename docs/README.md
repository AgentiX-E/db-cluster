# db-cluster — Raft Consensus Cluster

TypeScript distributed consensus layer built on @maboke123/raft-core with mTLS transport, SQLite storage, HA failover, and comprehensive test coverage.

## Architecture

See [architecture.md](./architecture.md) for:
- Component diagram with all modules
- Data flow diagrams (propose, receive, heartbeat, connection lifecycle)
- Error state machine with error propagation paths
- Wire format and data structures
- Test architecture overview
- Design decisions (D1–D9) with rationale

## Test Suites

| Category | Directory | Count | Description |
|----------|-----------|-------|-------------|
| Unit | `src/__tests__/unit/` | 26 files | Pure function + mock-based, no network |
| Integration | `src/__tests__/integration/` | 5 files | Real 3-node mTLS clusters in-process |
| Performance | `src/__tests__/performance/` | 3 files | Throughput, election, chaos engineering |
| Interface | `src/__tests__/interfaces/` | 12 files | Type-level contract verification |

### Running Tests

```bash
# Full suite
npx vitest run

# By category
npx vitest run src/__tests__/unit/
npx vitest run src/__tests__/integration/
npx vitest run src/__tests__/performance/

# Single file
npx vitest run src/__tests__/unit/RaftAdapter.test.ts

# With visible Raft logs
DEBUG=1 npx vitest run src/__tests__/performance/raft-chaos.test.ts
```

### Current Coverage

- **43 test files, 391 tests, 0 failures**
- All source files referenced by at least one test
- Real 3-node in-process mTLS clusters in integration + performance suites
- Chaos engineering: leader kill, follower kill, leader flapping verification

## Key Design Decisions

1. **Pool:forks** — Tests run in separate processes for true isolation
2. **No `vi.useFakeTimers`** — Real timers avoid hangs with Raft's `setInterval`-based health checks
3. **Per-class mock strategy** — Pure function tests in separate files (e.g., `ConnectionPoolFrames.test.ts`), integration tests exercise full paths
4. **`encodeFrame`/`decodeFrame` tested in isolation** — Wire format verified without TLS dependency

## Commit History

```bash
7704a60  feat: complete test coverage + architecture docs
88f1736  feat: add throughput benchmark + fix stress test flakiness
```

Author: Lambertyan <lambertyan@users.noreply.github.com>

# Changelog

All notable changes to @agentix-e/db-cluster will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-07-26

### Added

- **RaftAdapter** — Raft consensus adapter wrapping @maboke123/raft-core
  - proposeAndWait concurrency semaphore (CONCURRENCY_MAX=128)
  - Queue-pipeline backpressure pattern
  - Force leader bypass for HA failover
- **RaftGroupManager** — Multi-RaftGroup lifecycle and message routing
  - Per-node multi-shard replica hosting
  - Message routing by shard ID
  - Health status monitoring
- **JointConsensusEngine** — Two-phase membership changes (Raft §6)
  - C_old → C_joint → C_new transition
  - Retry with exponential backoff
  - Timeout-based automatic rollback
  - Event-driven progress notifications
- **Transport Layer** — Multi-transport architecture
  - RaftTcpTransport with mTLS support
  - BatchedRaftTransport for connection reuse
  - SharedBatchedTransport for single-process multi-RaftGroup
  - NodeTransport for inter-group routing
  - ConnectionPool with connection-storm resilience
  - MessageCodec for wire protocol serialization
- **Storage Layer** — SQLite-backed WAL
  - SqliteStorage with automatic log compaction
  - SqliteNodeStorage adapter for @maboke123/raft-core
- **Snapshot Support** — InstallSnapshot RPC with chunked streaming
- **Leadership Transfer** — Graceful handoff with NOOP barrier and drain phase
  - Heartbeat stop to trigger election
  - Poll-based new leader detection
  - 10s timeout with configurable retry
- **Read Path Optimizations**
  - LeaseReadManager — leader lease for linearizable reads without quorum round-trip
  - ReadIndexManager — full ReadIndex protocol fallback
- **HA Failover** — Production-grade high availability
  - HAFailoverDetector — dead leader detection with configurable thresholds
  - HAFailoverCoordinator — coordinated failover with fencing tokens
  - HAConsistencyVerifier — post-failover consistency checks
  - ClockCompensator — clock skew compensation for HLC
- **Fencing** — FencingManager with epoch-based token generation
- **Adaptive Election Timeout** — Jitter-based election tick randomization
- **Circuit Breaker** — Transport-level circuit breaker pattern
- **Message Compression** — Payload compression for large Raft messages
- **TLA+ Formal Verification**
  - JointConsensus.tla — two-phase membership change specification
  - JointConsensusElection.tla — election safety specification
  - TLC model checking with multiple trace outputs
- **Testing Suite**
  - 613 tests, 72 test files
  - Boundary tests — partition, compaction, combo-fault, security
  - LTS (Long-Term Stability) soak tests
  - Jepsen-style fuzzy register validation
  - Performance baseline benchmarks
  - Chaos engineering with fault injection

### Fixed

- RPC timeout races resolved via 16-way batch Promise.all pattern
- Transport connection-storm resilience during cluster startup
- Array.find async callback bug in batch proposal (regression D2.6-2.9)
- LTS cluster init retry (3 attempts) for TCP TIME_WAIT port conflicts
- Adaptive 5-minute stability cycle with single-core thresholds
- Boundary test cross-test state pollution via per-test cluster isolation

[0.1.0]: https://github.com/AgentiX-E/db-cluster/releases/tag/v0.1.0

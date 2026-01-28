# Designing Data-Intensive Applications — Implementation

<p align="center">
  <img src="./image.png" alt="DDIA Implementation">
</p>

A hands-on exploration of distributed systems concepts from Martin Kleppmann's *Designing Data-Intensive Applications*. Each module implements core ideas from the book with working Python code.

## Modules

| Module | Topic | Key Concepts |
|--------|-------|--------------|
| [ReliabilityService](./ReliabilityService/) | Reliability & Fault Tolerance | Circuit breakers, rate limiting, retries with backoff |
| [MultiModelApi](./MultiModelApi/) | Data Models | Relational, document, and graph storage with unified API |
| [StorageEngine](./StorageEngine/) | Storage Engines | Log-structured storage, LSM-trees, SSTables, Bloom filters |
| [VersionedEncoding](./VersionedEncoding/) | Encoding & Evolution | Schema versioning, forward/backward compatibility |
| [ReplicatedStore](./ReplicatedStore/) | Replication | Leader-follower replication, WAL, consistency guarantees |
| [ShardedStore](./ShardedStore/) | Partitioning | Consistent hashing, shard routing, rebalancing |
| [TransactionSystem](./TransactionSystem/) | Transactions | MVCC, isolation levels, serializable snapshot isolation |
| [PartitionFailures](./PartitionFailures/) | Distributed Failures | Network partitions, failure detectors, CRDTs |
| [ConsensusStore](./ConsensusStore/) | Consensus | Raft algorithm, replicated state machines |

## Quick Start

Each module is self-contained. No external dependencies required beyond Python 3.8+.

```bash
# Run any module directly
cd ReliabilityService
python server.py &
python client.py

# Or run the tests/demos
cd TransactionSystem
python anomalies.py
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                             │
├─────────────────────────────────────────────────────────────────┤
│  Circuit Breakers │ Rate Limiters │ Retry Policies │ Routing    │
├─────────────────────────────────────────────────────────────────┤
│                      Consensus (Raft)                            │
├─────────────────────────────────────────────────────────────────┤
│  Replication │ Partitioning │ Transactions │ Failure Detection  │
├─────────────────────────────────────────────────────────────────┤
│                      Storage Engines                             │
│  Log-Structured │ LSM-Trees │ SSTables │ Compaction │ Indexing  │
├─────────────────────────────────────────────────────────────────┤
│                      Encoding Layer                              │
│  Schema Evolution │ Binary Encoding │ Versioning                │
└─────────────────────────────────────────────────────────────────┘
```

## Module Highlights

### Reliability Patterns
Implements production-ready patterns for handling failures gracefully—circuit breakers that trip after consecutive failures, sliding-window rate limiters, and exponential backoff with jitter.

### Multi-Model Storage
A unified API that works with relational (SQLite), document (JSON files), and graph (in-memory with adjacency lists) backends. Demonstrates how the same data can be modeled differently.

### Log-Structured Storage
A complete storage engine with append-only log segments, sparse indexes, Bloom filters for fast negative lookups, and background compaction to reclaim space.

### Schema Evolution
Binary encoding format supporting schema versioning with forward and backward compatibility. New fields get defaults, removed fields are ignored, type changes are detected.

### Replication
Leader-follower replication with write-ahead logging, configurable sync modes (async, semi-sync), and read-your-writes consistency for clients.

### Partitioning
Consistent hashing with virtual nodes for even distribution. Supports adding/removing shards with automatic data migration and rebalancing.

### Transactions
Full MVCC implementation with multiple isolation levels: read uncommitted, read committed, repeatable read, and snapshot isolation. Plus serializable snapshot isolation (SSI) that detects write skew anomalies.

### Failure Handling
Network partition simulation, phi accrual failure detector for adaptive timeouts, gossip-based failure detection, and CRDT counters (G-Counter, PN-Counter) that converge after partitions heal.

### Consensus
Complete Raft implementation with leader election, log replication, and safety guarantees. Includes persistent log, metadata storage, and pluggable state machines.

## Learning Path

Suggested order for working through the modules:

1. **StorageEngine** — Start with how data is actually stored
2. **VersionedEncoding** — Understand data serialization
3. **ReliabilityService** — Build resilient clients
4. **MultiModelApi** — Compare data modeling approaches
5. **ReplicatedStore** — Add redundancy
6. **ShardedStore** — Scale horizontally
7. **TransactionSystem** — Ensure correctness
8. **PartitionFailures** — Handle the inevitable
9. **ConsensusStore** — Achieve distributed agreement

## References

- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly Media.
- Ongaro, D., & Ousterhout, J. (2014). In Search of an Understandable Consensus Algorithm (Raft).
- Shapiro, M., et al. (2011). Conflict-free Replicated Data Types.



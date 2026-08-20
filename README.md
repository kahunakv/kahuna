# 🦎 Kahuna

<img src="https://github.com/user-attachments/assets/d73a177f-5b9a-4e63-9b8d-9bcf067da002" height="250" alt="kahuna">

Distributed systems are difficult to build correctly. Execution can be non-deterministic. Edge cases are hard to predict. These factors make it difficult to reason about correct solutions.

Kahuna is an open-source project. It gives developers ready-made solutions for three common problems in distributed systems:

- Distributed locking
- A distributed key/value store
- A distributed sequencer

### **Distributed Locking**

Multiple nodes or processes often need access to the same resource. Kahuna synchronizes that access to prevent race conditions and to keep data consistent.

[More](https://kahunakv.github.io/docs/distributed-locks)

### **Distributed Key/Value Store**

Kahuna stores and retrieves structured data across a cluster. The store is fault-tolerant and has high throughput. Use it for metadata, caches, or application state.

[More](https://kahunakv.github.io/docs/distributed-keyvalue-store)

### **Distributed Sequencer**

Kahuna generates globally ordered identifiers. Distributed databases, message queues, and event-driven systems use these identifiers to order their operations.

[More](https://kahunakv.github.io/docs/distributed-sequencer)

These three capabilities work together. They give you a foundation for reliable and scalable distributed applications.

> _Kahuna_ is a Hawaiian word for an expert in any field.
Historically, it referred to doctors, surgeons, dentists, priests, ministers, and sorcerers.

Read the [documentation](https://kahunakv.github.io/) for architecture details, installation steps, and usage examples.

## Installation

The quickest way to start a node is the .NET global tool:

```bash
dotnet tool install -g Kahuna.Server
kahuna-server
```

With no arguments, this command starts a standalone node on HTTP port 2070. The node stores
key-value data and the Raft write-ahead log under the per-user data directory:

- Linux / macOS: `~/.local/share/kahuna`
- Windows: `%LOCALAPPDATA%\kahuna`

The node prints both paths at startup. Set `KAHUNA_HOME` to change the location. You can also
pass `--storage-path` or `--wal-path` directly.

A node started this way serves HTTP only. HTTPS binds only when you supply a certificate:

```bash
kahuna-server --https-certificate /path/to/certificate.pfx --https-ports 2071
```

The command-line client is a separate tool:

```bash
dotnet tool install -g Kahuna.Control
kahuna-cli
```

For a multi-node cluster, or to build from source, see the Docker images under `docker/` and the
scripts in `scripts/`.

## Architecture

<img src="https://github.com/user-attachments/assets/b60b213c-d12d-48a5-ba22-38fe99d2a590" height="350">

### Distributed Storage Engine

Kahuna is a scalable, fault-tolerant distributed system. It combines lock management, key-value
storage, and a sequencer.

Data is organized into partitions. A partition is an independent shard that can be distributed
across the node cluster. The system moves and manages each partition independently.

Kahuna uses Multi-Version Concurrency Control (MVCC). MVCC keeps multiple versions of each value.
This makes snapshot isolation possible: read operations return consistent data even while
concurrent writes change the same keys. MVCC eliminates read-write conflicts that would otherwise
reduce throughput.

### Raft-Based Consensus

Each partition has its own Raft group. Raft is a consensus protocol. It replicates all changes
across multiple nodes and provides fault tolerance and high availability.

Within each Raft group, one node is elected leader. The leader coordinates all write operations
for its partition. Every write becomes a log entry. The leader replicates each log entry to
follower nodes. This process keeps data consistent across all nodes that hold a given partition.

### Transactional Model

Kahuna combines a two-phase commit (2PC) protocol with MVCC. A transaction proceeds in two phases:

1. **Prewrite** — the system acquires locks on the affected keys and records tentative writes.
2. **Commit** — the system finalizes the changes across replicas. The transaction completes
   atomically.

Kahuna supports two concurrency control modes:

- **Optimistic** — transactions read from a consistent snapshot. The system checks for conflicts at
  commit time. This mode is faster when conflicts are rare.
- **Pessimistic** — the system acquires locks before it modifies keys. This mode prevents conflicts
  on contended keys.

### Scalability and Fault Tolerance

Kahuna scales horizontally through dynamic partition management. Partitions split and redistribute
across nodes automatically to balance load. More nodes give proportionally more capacity.

Raft-based replication ensures high availability. The system continues to operate when individual
nodes fail. Data stays accessible through replicas. The recovery process restores consistency
after failures and does not lose committed transactions.

### Performance Optimizations

Kahuna maintains strong consistency through Raft. It also applies several optimizations. Where
appropriate, asynchronous replication reduces read latency without a loss of consistency.

Background processes run compaction and garbage collection continuously. These processes reclaim
storage space and memory. They remove obsolete data versions that the system no longer needs for
transaction isolation or recovery.

---

## Running Tests

The `Kahuna.Client.Tests` project holds end-to-end tests that connect to a live Kahuna cluster.
Start the Docker cluster before you run those tests:

```bash
docker compose -f docker/local.yml up -d
```

The client tests expect HTTPS endpoints on:

```text
https://localhost:8082
https://localhost:8084
https://localhost:8086
```

Then run the tests:

```bash
dotnet test Kahuna.Client.Tests/Kahuna.Client.Tests.csproj
```

When you finish, stop the cluster:

```bash
docker compose -f docker/local.yml down
```

The `Kahuna.Server.Tests` project uses embedded, in-process nodes. It does not need the Docker
cluster:

```bash
dotnet test Kahuna.Server.Tests/Kahuna.Server.Tests.csproj
```

GitHub Actions starts the server cluster before it runs the end-to-end suite. The startup script
is `scripts/run-server.sh`.

---

## Jepsen Tests

Kahuna is tested with [Jepsen](https://jepsen.io/). Jepsen is a framework that verifies
correctness of distributed systems under real-world failures: network partitions, process crashes,
and clock skew.

The test suite lives at [kahunakv/kahuna-jepsen](https://github.com/kahunakv/kahuna-jepsen). It
exercises transactional guarantees, lock semantics, and replication behavior. The suite injects
faults into a cluster and checks that the observed history stays consistent.

These tests verify that Kahuna upholds its safety properties — serializability, linearizability,
and durability — under adversarial conditions, not only on the happy path.

---

## Kubernetes Operator (Alpha)

The [Kahuna Kubernetes Operator](https://github.com/kahunakv/kahuna-k8s-operator) automates
deployment and management of Kahuna clusters on Kubernetes. It provisions clusters, scales them,
and manages their lifecycle through a custom resource definition. You can run Kahuna as a native
Kubernetes workload.

> **Note:** This operator is in alpha. APIs and behavior can change between releases.

---

## Contributing

We welcome contributions from the community. For detailed guidelines,
refer to our [CONTRIBUTING.md](CONTRIBUTING.md) file.

---

## License

Kahuna is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

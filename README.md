# 🦎 Kahuna

<img src="https://github.com/user-attachments/assets/d73a177f-5b9a-4e63-9b8d-9bcf067da002" height="250" alt="kahuna">

Distributed systems can become highly complex due to the many reasons: execution may be non-deterministic, unexpected edge cases, and specific scenarios that make it difficult to reason about solid solutions that ensure system robustness.

Kahuna is an open-source project aimed at providing out-of-the-box solutions for developers and applications that need to solve common problems related to distributed systems.

It is primarily focused on the following areas: distributed locking, a distributed key/value store and a distributed sequencer.

### **Distributed Locking**
Kahuna addresses the challenge of synchronizing access to shared resources across multiple nodes or processes, ensuring consistency and preventing race conditions. Its locking mechanism ensures efficient coordination for many use cases.

[More](https://kahunakv.github.io/docs/distributed-locks)

### **Distributed Key/Value Store**
Beyond locking, Kahuna operates as a distributed key/value store, enabling fault-tolerant, 
high-performance storage and retrieval of structured data. This makes it a powerful tool 
for managing metadata, caching, and application state in distributed environments.

[More](https://kahunakv.github.io/docs/distributed-keyvalue-store)

### **Distributed Sequencer**
Kahuna also functions as a distributed sequencer, providing a globally ordered execution 
of events or transactions. This capability is essential for use cases such as distributed 
databases, message queues, and event-driven systems that require precise ordering of 
operations.

[More](https://kahunakv.github.io/docs/distributed-sequencer)

By seamlessly integrating these three functionalities, Kahuna provides a comprehensive 
foundation for building reliable and scalable distributed applications.

> _Kahuna_ is a Hawaiian word that refers to an expert in any field. Historically,
it has been used to refer to doctors, surgeons and dentists,
as well as priests, ministers, and sorcerers.

Check the [documentation](https://kahunakv.github.io/) for more information on architecture, installation, and usage examples.

## Installation

The quickest way to get a node running is the .NET global tool:

```bash
dotnet tool install -g Kahuna.Server
kahuna-server
```

With no arguments this starts a standalone node on HTTP port 2070. Its key-value data and Raft
write-ahead log go under the per-user data directory — `~/.local/share/kahuna` on Linux/macOS,
`%LOCALAPPDATA%\kahuna` on Windows — and both resolved paths are printed at startup. Set
`KAHUNA_HOME` to relocate them, or pass `--storage-path` / `--wal-path` explicitly.

A node started this way serves **HTTP only**: HTTPS binds only when you supply a certificate.

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

Kahuna's architecture operates as a highly scalable, fault-tolerant distributed system that combines lock management, key-value storage, and sequencing capabilities. At its foundation lies a distributed key-value storage model where data is organized into discrete partitions similar to sharding mechanisms in other distributed systems. These partitions function as independent units that can be distributed and managed across the entire node cluster.

The system implements Multi-Version Concurrency Control (MVCC) to maintain multiple versions of data simultaneously. This versioning mechanism enables snapshot isolation for transactions, allowing the system to provide consistent read operations even while concurrent write operations are being processed on the same data. This approach eliminates read-write conflicts that would otherwise impact performance in high-concurrency environments.

### Raft-Based Consensus
Consensus across the distributed system is achieved through the Raft protocol, with each partition in Kahuna being governed by its own Raft group. This protocol ensures consistent replication of all changes across multiple nodes, thereby establishing the foundation for Kahuna's fault tolerance and high availability characteristics.

Within each Raft group, the consensus mechanism designates one node as the leader through an election process. This leader node coordinates all write operations for its assigned partition. To maintain consistency, all operations are recorded as log entries which are systematically replicated to follower nodes. This replication process ensures that data remains consistent across all nodes responsible for a particular partition.

### Transactional Model
Kahuna implements a transaction management system that combines a two-phase commit protocol with MVCC. This transactional framework operates in distinct phases:

During the prewrite phase, locks are acquired on affected keys and tentative write operations are recorded in the system but not yet confirmed. Following successful preliminary operations, the commit phase activates, during which the system finalizes these changes across replicas to ensure atomic transaction completion.

The system supports both optimistic and pessimistic concurrency control approaches. With optimistic concurrency control, transactions operate on consistent snapshots of data while deferring conflict resolution until commit time. This approach optimizes performance in scenarios where conflicts are rare. Alternatively, when using pessimistic concurrency control, locks are acquired in advance of modifications, effectively preventing conflicts that might otherwise arise from concurrent operations on identical keys.

### Scalability and Fault Tolerance
Horizontal scalability is achieved through dynamic partition management. Partitions can be automatically split and redistributed across nodes to achieve optimal load balancing. This architecture can support linear scalability as additional nodes are integrated into the cluster, allowing Kahuna to expand its capacity proportionally with infrastructure growth.

High availability is ensured through Raft-based replication mechanisms. The system maintains operation even when individual nodes fail, as data remains accessible through replicas. Kahuna's recovery processes are designed to restore system integrity after failures without compromising committed transactions, maintaining both data consistency and service availability.

### Performance Optimizations
While Kahuna maintains strong consistency guarantees through the Raft protocol, it also incorporates various performance optimizations. Asynchronous replication techniques are employed where appropriate to enhance data replication efficiency and minimize read operation latency without sacrificing consistency requirements.

Background maintenance processes continuously perform compaction and garbage collection operations to reclaim storage space and memory resources. These automated maintenance routines help preserve system performance by systematically removing obsolete data versions that are no longer needed for transaction isolation or recovery purposes.

---

## Running Tests

The `Kahuna.Client.Tests` project holds end-to-end tests that connect to a running Kahuna
cluster. Start the Docker cluster before running those tests locally:

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

When finished, stop the cluster with:

```bash
docker compose -f docker/local.yml down
```

The `Kahuna.Server.Tests` project uses embedded, in-process nodes and does not require the
external Docker cluster:

```bash
dotnet test Kahuna.Server.Tests/Kahuna.Server.Tests.csproj
```

GitHub Actions starts the server cluster before running the end-to-end suite through
`scripts/run-server.sh`.

---

## Jepsen Tests

Kahuna is tested with [Jepsen](https://jepsen.io/), a framework designed to verify the correctness of distributed systems under real-world failure conditions such as network partitions, process crashes, and clock skew.

The Jepsen test suite for Kahuna lives at [kahunakv/kahuna-jepsen](https://github.com/kahunakv/kahuna-jepsen). It exercises Kahuna's transactional guarantees, locking semantics, and replication behavior by injecting faults into a running cluster and checking that the system's observable history remains consistent. These tests are essential for building confidence that Kahuna upholds its safety properties — serializability, linearizability, and durability — not just in the happy path but under adversarial conditions that are difficult to reproduce with conventional test suites.

---

## Kubernetes Operator (Alpha)

The [Kahuna Kubernetes Operator](https://github.com/kahunakv/kahuna-k8s-operator) automates deploying and managing Kahuna clusters on Kubernetes. It handles cluster provisioning, scaling, and lifecycle operations through a custom resource definition, letting you run Kahuna as a native Kubernetes workload.

> **Note:** This operator is currently in alpha. APIs and behavior may change between releases.

---

## Contributing

We welcome contributions from the community! For detailed guidelines, 
refer to our [CONTRIBUTING.md](CONTRIBUTING.md) file.

---

## License

Kahuna is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


# 🦎 Kahuna

<img src="https://github.com/user-attachments/assets/d73a177f-5b9a-4e63-9b8d-9bcf067da002" height="350" alt="kahuna">

Kahuna is an open-source solution designed to help developers coordinate modern distributed systems by integrating three 
critical functionalities: **distributed locking, a distributed key/value store and a distributed sequencer.**

By enabling synchronized access to shared resources, efficient data storage and retrieval, and globally ordered event sequencing, 
Kahuna offers a new approach to managing distributed workloads. Built on a partitioned architecture coordinated via **Raft Groups**, 
it delivers scalability, reliability and simplicity, making it a great choice for 
applications requiring strong consistency and high availability.

### **Distributed Locking**
Kahuna addresses the challenge of synchronizing access to shared resources across multiple 
nodes or processes, ensuring consistency and preventing race conditions. Its partitioned locking 
mechanism ensures efficient coordination for databases, files, and other shared services.

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

## Architecture

<img src="https://github.com/user-attachments/assets/c2bf6c5e-f918-44de-a394-99921642f706" height="350">

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

## Contributing

We welcome contributions from the community! For detailed guidelines, 
refer to our [CONTRIBUTING.md](CONTRIBUTING.md) file.

---

## License

Kahuna is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.



# Kahuna storage engines guide

This guide explains how a Kahuna node chooses the engines that store its data, and how a host adds an
engine of its own. It is written for two audiences:

- **Operators** who choose an engine with `--storage` and `--wal-storage`.
- **Developers** who write a storage engine outside this repository, or maintain the code that opens one.

---

## 1. The two engines of a node

A node stores two things, each in an engine chosen by name:

| Option | Embedded option | Stores | Built-in names |
| --- | --- | --- | --- |
| `--storage` | `EmbeddedKahunaOptions.Storage` | the key-value and lock data (the persistence backend) | `rocksdb`, `sqlite`, `memory` |
| `--wal-storage` | `EmbeddedKahunaOptions.WalStorage` | Kommander's Raft log (the WAL) | `rocksdb`, `sqlite`, `memory` |

The names are looked up in a `StorageProviders` registry (`Kahuna.Server.Persistence.Providers`). A name
that nothing registered stops the node when it starts, with the list of the names that are registered.
The thread-free browser build registers `memory` only.

## 2. Adding an engine

An engine is an `IPersistenceBackendProvider` (for `--storage`) or an `IWalProvider` (for `--wal-storage`).
A host registers it on the options before it constructs the node; a registration under a name that exists
replaces the built-in one.

```csharp
EmbeddedKahunaOptions options = new()
{
    Storage = "myengine",
    StoragePath = "/var/lib/kahuna/data",
    WalStorage = "rocksdb",
    // ...
};
options.StorageProviders.Register(new MyEngineProvider());

await using EmbeddedKahunaNode node = new(options, loggerFactory);
```

A clustered server takes its registry from `Program.cs`, which creates one and hands the same instance to
both node shapes and to the WAL, so a standalone and a clustered node open the same engines.

### From the command line: extension assemblies

An operator who runs the shipped server adds an engine with `--extension-assembly`, the flag that also
loads user-defined script functions (see the user-defined functions guide). The server loads the assembly
before it builds the node and registers every public, non-abstract `IPersistenceBackendProvider` and
`IWalProvider` in it that has a parameterless constructor; one assembly may publish functions, engines, or
both.

```sh
kahuna-server --extension-assembly /opt/kahuna/engines/MyEngine/MyEngine.dll \
              --storage myengine --storage-path /var/lib/kahuna/data
```

- **Dependencies.** Publish the engine with its dependencies (`dotnet publish`) and point the flag at the
  assembly in that directory. The server loads each dependency from there, as the assembly's
  `.deps.json` lists it, except an assembly the server has itself (Kahuna, Kommander, the runtime and
  their dependencies): that one is the server's copy, so the engine's types implement the server's
  interfaces. An engine therefore builds against the Kahuna.Core version the server runs.
- **Names.** An engine may not take a type name that is registered already, built-in or from another
  extension: the node refuses to start instead of replacing it.
- **Every node.** Each loaded file is logged with its SHA-256 and the engines it registered. Every node of
  a cluster must load the same artifact, and a node restoring a backup must have the engine that wrote it.
- **Trust.** An extension assembly is fully trusted code that runs in the server process with the
  server's privileges.

### Settings for an engine

A provider has no constructor arguments, so an engine takes its settings from `--extension-option`, as
`key=value` pairs after one flag. The value is everything after the first `=`, and a key may appear once:

```sh
kahuna-server --extension-assembly /opt/kahuna/solarium/Solarium.Kahuna.dll \
              --storage solarium --wal-storage solarium \
              --extension-option solarium.environment=io_uring
```

An embedding host sets `EmbeddedKahunaOptions.ExtensionOptions`. The node passes the map, unread, to
`KahunaConfiguration.ExtensionOptions` and to the `ExtensionOptions` of every `PersistenceBackendContext` and
`WalContext` it opens, so an engine reads its own keys in `Open`. An engine should refuse, with an
exception that names the key, a value it cannot use, and ignore keys that are not its own. By convention,
an engine's keys start with its name, so the engines of several extensions do not collide. Every node of a
cluster should be given the same settings.

The backend itself implements `IPersistenceBackend` (`Kahuna.Server.Persistence.Backend`), the same
contract as the RocksDB, SQLite and memory backends. It returns immutable rows: `ReadOnlyKeyValueEntry`
for key-value rows and `LockRow` for locks. The node copies a row into its own state when it loads it, so
a backend can keep and share the rows it returns.

## 3. What a backend provider answers

Besides opening the node's backend, a provider tells the backup, restore and bootstrap paths what they
need to know about the engine's files:

| Member | Used by | What it says |
| --- | --- | --- |
| `Open(context)` | node start | opens the backend at `context.StoragePath` under `context.StorageRevision` |
| `KeepsDataOnDisk` | node start, embedded clusters, PITR bootstrap, restore | the data lives in files under the storage path: the node creates the path, puts snapshot staging under it, and can install a base image into it. False only for an engine that holds everything in memory. |
| `OpenAt(directory, revision, logger)` | restore | opens a backend over a restore's staging copy of a checkpoint |
| `DataDirectory(root, revision)` | restore, PITR bootstrap, live checkpoint staging | where the store's files live under a storage root: the root itself (SQLite, whose file names carry the revision) or a directory under it (RocksDB's `{root}/{revision}`) |
| `HasData(root, revision)` | PITR bootstrap | the files the engine would open exist already, so the store belongs to a node that ran and is never overwritten |
| `MergeCheckpointInto(target, revision, source, ct)` | coordinated backups | adds every row of one node's checkpoint share to another's; the shares hold disjoint keys |
| `AdoptRevision(directory, revision)` | coordinated backups | renames a checkpoint's files to the local revision, for an engine whose file names carry it (default: nothing) |
| `StagesLiveCheckpointsInStore` | local backups | a live checkpoint is staged inside the store's directory, for an engine whose checkpoint links files and so needs the same file system (RocksDB; default: false) |

`PersistenceBackendContext` carries the node's `KahunaConfiguration` for settings an engine reads, and,
when both the backend and the WAL are RocksDB with the shared memory budget on, the RocksDB shared
resources (see the shared RocksDB memory guide). `WalContext` carries the WAL path, revision, sync mode
and logger, and the RocksDB WAL's tuning and shared resources, which other engines ignore.

## 4. Backups of a registered engine

A backup records the storage type that wrote it. A restore opens the checkpoint through the provider of
that type, so a node that restores a backup must have the same engine registered; one that does not
refuses the restore with the engine's name. The checkpoint itself is the engine's own format: the backend's
`CreateCheckpoint` and `CreateCheckpointAsOf` write it, and only that engine reads it back.

Where a backup goes is a separate choice, the backup target (`--pitr-backup-target`). A target is in the same
registry and loads from an extension assembly in the same way. See the backup targets guide.

## 5. Pacing an engine's compactions

Two options bound the background work of the key-value and lock store. They are 0 by default, and with
both unset every engine keeps its own behavior.

| Option | Embedded option | What it does |
| --- | --- | --- |
| `--storage-compaction-rate-mbps` | `EmbeddedKahunaOptions.StorageCompactionRateBytesPerSec` (bytes per second; the option takes MB/s) | A throughput budget for the store's flush and compaction writes. The WAL's writes and the reads are not charged. 0: unlimited. |
| `--storage-max-background-compactions` | `EmbeddedKahunaOptions.StorageMaxBackgroundCompactions` | The most compactions that run at once. 0: the engine's default. |

The store's writes are synced, so the disk's sync throughput sets their latency, and the store shares it
with its own flushes and compactions. Several compactions running at once can take most of a slow disk and
leave the synced writes waiting, even though they do not add much work in total. A rate keeps them to a
share. Choose a fraction of what the disk commits in synced writes, about a third to a half: too high a rate
protects nothing, and a rate too low for the write load lets level 0 grow until writes slow down and stop.

- **`rocksdb`** gives the rate to a RocksDB rate limiter that charges writes only (`kWritesOnly`, a 100 ms
  refill period), as RocksDB's `rate_limiter`. Its compactions default to `max(2, CPUs / 2)`.
- **`sqlite` and `memory`** do not compact, and ignore both.
- **An engine added by an extension** reads both from `PersistenceBackendContext.Configuration` and may
  honor them. The `solarium` engine's own documentation says what it does with them.

`--backup-capture-throttle-mbps` is separate: it paces the disk work of a backup capture, the as-of trim of
its image included, and not the live store.

## 6. Sharing memory between the WAL and the store

Two options give the WAL and the store of a node memory to share. Both are 0 by default, and each engine then
keeps its own.

| Option | Embedded option | What it does |
| --- | --- | --- |
| `--storage-shared-memtable-budget-mb` | `EmbeddedKahunaOptions.StorageSharedMemtableBudgetMb` | One memtable budget: the memtables of both stay under it together, and an engine flushes a memtable early when they pass it. |
| `--storage-shared-cache-mb` | `EmbeddedKahunaOptions.StorageSharedCacheMb` | One cache for both. With a memtable budget too, the budget is charged into the cache, so one size bounds both, and it must be at most the cache. |

The node creates one `SharedStorageMemory` with both sizes and hands the same instance to both providers, as
`WalContext.SharedMemory` and `PersistenceBackendContext.SharedMemory` (null when both are 0). A WAL and a
store of the same kind share their accounting objects through it: each asks `GetOrAdd<T>(memory => ...)` for
its engine's own type, the first to ask creates it, and both get the same one. A pair of different kinds each
gets its own objects of the same sizes.

- **`rocksdb`** applies them only with `--rocksdb-shared-memory`, as its shared block cache and write-buffer
  manager, which charges the memtables into the cache; it then requires both above 0 (see the shared RocksDB
  memory guide).
- **`sqlite` and `memory`** have no memtables or cache, and ignore them.
- **An engine added by an extension** reads them from the context. The `solarium` engine's own documentation
  says what it does with them.

## 7. Storage damage and the store identity

The RocksDB store checks every block it reads (XXH3 checksums, `format_version=7`, so a checksum also
covers the block's file and offset). A flipped bit, a misdirected write or a truncated file is found by
the next read of the block, as a status whose code is `Corruption`. What the node does with that finding
is a contract CamusDB's storage-corruption campaign relies on; SQLite has no page checksums and is outside
it.

### A damaged store fail-stops

The first `Corruption` the store meets — on a point read, a batched read, a range scan (an iterator that
reaches a damaged block parks the status instead of throwing; every scan ends through a check that
rethrows it, so a scan never returns a short page as if the range ended at the damage), on a write after a
compaction latched it as the engine's background error, or at the open — fail-stops the store for the rest
of the process:

- every later operation is refused with `StorageCorruptionException`; the store methods return `false`,
  and the background writer stops retrying and stops asking for the ENOSPC recovery;
- the recovery never closes or reopens a damaged store, so the files stay exactly as they are for
  forensics (the reopen built for a full volume would meet the damage again on the next read or
  compaction and loop);
- the engine's message, which names the file and the block, is logged once at Critical and is readable
  from `IPersistenceBackend.GetFailStopReason` and from the node's `IKahuna.GetStorageHealth()`;
- `kahuna.persistence.store_fail_stops_total` counts it, once per process.

The node answers the read that found the damage, and every read and write of its partitions after it,
with `StorageCorrupt` (key-value response type 115, lock response type 105): a terminal answer, deliberately
distinct from `MustRetry`, so a client does not spend its retry budget against a node that cannot change
its answer. Nothing read from the damaged store is ever served. The node then withholds its candidacy for
every partition it hosts and steps down where it leads (the gate is tripped by the first classified read
and re-asserted on each writer tick), so a replicated partition moves to another replica; a single-replica
partition stays unavailable until the operator acts. `/v1/cluster/health` answers 503 with `storageFault`
set to the engine's message.

Repair is the operator's: restore the store from a backup, or move the damaged directory aside and restart
the node so it is rebuilt from its peers (see the next section for what the node does with an empty
store). A store whose files fail their checksums at open, or whose own durability-floor rows cannot be
decoded (a floor row that is not 8 bytes is damage, never "no floor"), refuses the open: the node does not
start over it.

### The store identity

A RocksDB store belongs to one node's Raft log: its durability floors certify that log's entries. The
first time a node opens a store it stamps it with its Raft endpoint, a creation id and a creation time
(`PersistentStoreIdentity`, kept in a reserved row that user scans never see). At every later open, before
Kommander consults any floor, the node checks the stamp:

| What it finds | What it does |
| --- | --- |
| its own stamp | opens the store |
| another node's stamp | refuses to start (`StoreIdentityMismatchException`): the floors would vouch for a different log, and Kommander would skip replay over entries this copy never applied |
| no stamp, but rows or floors | a store written by a build that predates the stamp: stamps it and opens it |
| no stamp, no rows, a Raft log without history | a fresh node: stamps it |
| no stamp, no rows, a Raft log with history | the store directory was lost or replaced: logs it at Critical, counts `kahuna.persistence.store_replaced_at_start_total`, reports it in `/v1/cluster/health` (`storageFault`, 503), stamps the store and starts |

In the last case the empty store answers a durability floor of `0` for every data partition ("nothing
durably applied"; the system partition keeps "no opinion", since its range-map and hold-registry entries
are not floor-tracked). Kommander then replays the whole log where it still holds it, and where the log
was compacted below what the store needs it refuses to replay the surviving suffix onto a store missing
its prefix: the partition withholds its candidacy, keeps voting, and asks its leader for a whole-partition
snapshot, the only repair. A single-replica partition has no leader to ask and stays quarantined, never
serving partial data, until the store is restored from a backup. Either verdict shows in
`/v1/cluster/health` as `partitionStorageFaults`.

Backup images carry no stamp: a checkpoint is trimmed of the identity row, and a coordinated backup never
merges one, so a node that restores or bootstraps from an image stamps its own and is never refused as
foreign.

An engine added by an extension takes part through the same members: `GetFailStopReason` for the fail-stop,
`KeepsDurabilityFloors` for the empty-store floor, and `SupportsStoreIdentity` / `GetStoreIdentity` /
`WriteStoreIdentity` / `HoldsDurableState` for the identity check. The defaults opt an engine out of all of
them.

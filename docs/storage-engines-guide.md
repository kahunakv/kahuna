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

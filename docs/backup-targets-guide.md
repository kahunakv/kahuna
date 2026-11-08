# Kahuna backup targets guide

This guide tells you how a Kahuna node chooses the place where it keeps its backups, and how to add a
place of your own. It is for two audiences:

- **Operators** who choose a backup target with `--pitr-backup-target`.
- **Developers** who write a backup target outside this repository, such as an object-storage adapter.

For backups, restores and the PITR bootstrap, see the backups and point-in-time recovery guide. This guide
covers only where the bytes go.

---

## 1. Targets by name

A backup target is a place for two kinds of objects:

- **Manifests.** A manifest is a small file that describes one backup: its artifacts and their digests.
- **Artifacts.** An artifact is a large file of backup data: a checkpoint file or a WAL segment.

The node chooses its target by name:

| Option | Embedded option | Default | Built-in names |
| --- | --- | --- | --- |
| `--pitr-backup-target` | `EmbeddedKahunaOptions.BackupTarget` | `local` | `local` |

The `local` target keeps manifests and artifacts in `--pitr-backup-dir` on this host. Another target, for
example a Google Cloud Storage bucket, comes from a separate assembly. `Kahuna.Core` never references a
cloud SDK, because every host that embeds the core would get that SDK too.

The names are in the `StorageProviders` registry (`Kahuna.Server.Persistence.Providers`). The storage
engines are in the same registry (see the storage engines guide). Names are compared ordinally, so `LOCAL`
is not `local`.

## 2. Startup checks

The node checks its target two times before it is ready. A backup target that cannot work stops the node at
startup. The node never comes up and then fails at the first scheduled backup.

1. **Validation**, when the node is built. The target name must be registered, and the error lists the
   registered names. If the provider says that it needs a scratch directory, `--pitr-backup-scratch-dir`
   must be set.
2. **Start**, before the node joins its cluster. The provider connects to the target and checks it, for
   example the credentials and the protections of a bucket. A failure stops the start, and the startup log
   shows the provider's message.

A node with no `--pitr-backup-dir` has backups off. It does not open a backup target, so it never calls an
extension's provider.

Before a provider opens the storage, Kahuna checks two directories:

- the backup directory, which must not be a symlink and must not be writable by the group or by others
- the scratch directory, with the same check. Kahuna then creates the scratch directory owner-only.

Kahuna does these checks itself, so a provider cannot skip them.

## 3. Loading a target from the command line

An operator who runs the shipped server adds a target with `--extension-assembly`. This is the same flag
that loads storage engines and user-defined script functions. The server registers every public,
non-abstract `IBackupStorageProvider` that has a parameterless constructor. The provider takes its settings
from `--extension-option`. By convention, each key starts with the target name.

```sh
kahuna-server --extension-assembly /opt/kahuna/backup-gcs/Kahuna.Storage.Gcs.dll \
              --pitr-backup-dir /var/lib/kahuna/backups \
              --pitr-backup-target gcs \
              --pitr-backup-scratch-dir /var/lib/kahuna/backup-scratch \
              --extension-option gcs.bucket=acme-kahuna-backups gcs.prefix=prod/
```

With Docker, mount the adapter bundle read-only into the container, for example at
`/opt/kahuna/backup-gcs`, and mount volumes for the backup and scratch directories. Then add the flags above
to the command that starts `Kahuna.Server.dll`. The entry point script of the standalone image
(`docker/entrypoint-standalone.sh`) has a fixed list of arguments and does not pass extra ones. So use your
own entry point script, or override the entry point.

Obey these rules:

- **Use the published bundle.** Give the flag the DLL from the output of `dotnet publish`, not the DLL from
  a NuGet package. The loader reads the `.deps.json` file next to the DLL to find the adapter's
  dependencies. Without that file, the dependencies do not load.
- **Match the server version.** The adapter always uses the server's copy of `Kahuna.Core`, Kommander and
  the other assemblies that the server has. Build the adapter against a `Kahuna.Core` version that is
  equal to or lower than the server's version. If the adapter needs a newer version, the node does not
  start. The message names the version that the adapter needs and the version that the server has.
- **One name, one target.** A target may not take a name that is already registered, including `local`.
  The node refuses to start, and does not replace the registered target.
- **The same on every node.** The startup log shows each loaded file with its SHA-256 and the names it
  registered, for example `backup:gcs`. Give every node of a cluster the same file and the same settings.
- **Trust.** An extension assembly is fully trusted code. It runs in the server process with the server's
  privileges.

## 4. Adding a target in an embedding host

An embedding host registers the provider on the options before it builds the node:

```csharp
EmbeddedKahunaOptions options = new()
{
    BackupDir = "/var/lib/kahuna/backups",
    BackupTarget = "gcs",
    BackupScratchDir = "/var/lib/kahuna/backup-scratch",
    // ...
};
options.StorageProviders.Register(new GcsBackupStorageProvider(new GcsBackupStorageOptions { Bucket = "acme-kahuna-backups" }));

await using EmbeddedKahunaNode node = new(options, loggerFactory);
await node.StartAsync(ct);   // starts the backup storage before the join
```

The provider in this example comes from the kahuna-gcs package. A provider that an embedding host builds
can take its settings as typed constructor arguments, so it does not need `ExtensionOptions`.

## 5. Writing a target

A target has two parts, both in `Kahuna.Server.Persistence.Pitr`:

| Type | What it does |
| --- | --- |
| `IBackupStorageProvider` | Has the target name and says if the target needs a scratch directory. `Open` reads and checks the settings, and returns an `IBackupStorage`. |
| `IBackupStorage` | The backup storage of one node. `StartAsync` connects and checks. `Manifests` and `Artifacts` are the two stores. `Dispose` releases the client. |

The stores implement `IBackupStorageTarget` (manifests) and `IBackupArtifactStore` (artifacts). The
reference implementations are `LocalDirectoryStorageTarget` and `LocalDirectoryArtifactStore`.

### The lifecycle

1. **`Open(context)`** runs when the node is built. It must not do I/O. It reads the settings from the
   context and throws `KahunaServerException` for a setting that it cannot use. The message must name the
   key, not the value, because a value can be a secret or a path.
2. **`StartAsync(ct)`** runs one time, before the node joins its cluster. Do all network and disk work here:
   build the client, load the credentials, check the target. The node can call it again after a failure.
   A node that was built without the start step, such as a test harness, starts the storage at its first
   backup operation.
3. **`Manifests` and `Artifacts`** are valid only after `StartAsync` completes.
4. **`Dispose()`** runs when the node stops. It can run while `StartAsync` is still in progress. In that
   case, release the client that `StartAsync` builds.

A PITR bootstrap runs before the node exists. It opens, starts and disposes a storage of its own from the
same provider. So `Open` can run two times in one process.

### The context

`BackupStorageContext` has these properties:

| Property | Contents |
| --- | --- |
| `BackupDir` | The backup directory. A remote target can keep host-side state here. |
| `ScratchDir` | The scratch directory, or null. It is never null when `RequiresLocalScratch` is true. |
| `Configuration` | The node's `KahunaConfiguration`. |
| `Logger` | The node's logger. |
| `ExtensionOptions` | The values of `--extension-option`. Read your own keys, and refuse an unknown key that starts with your name. |

### Scratch

A persistence backend writes a checkpoint only through the filesystem. So a target that is not a local
directory must stage each checkpoint in a local directory before it uploads it. Set `RequiresLocalScratch`
to true for such a target. The operator must then give a scratch directory that can hold one full backup.

### The binary contract

A target is compiled apart from the server that loads it. `IBackupStorageProvider`, `IBackupStorage`,
`IBackupStorageTarget` and `IBackupArtifactStore` are therefore a binary contract. When Kahuna adds a member
to one of them, the member has a default body, so a target that is already built continues to load.


using System.Text.Json;
using System.Text.Json.Serialization;
using Kommander.Time;

using Kahuna.Server.Persistence.Providers;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>What a node started with <c>--pitr-bootstrap-from</c> does once its base image step is settled.</summary>
public enum PitrBootstrapAction
{
    /// <summary>The store holds the full backup's base image: replay the chain and seed the WAL, then join.</summary>
    Seed,

    /// <summary>The store already holds data (or a completed seed): do not seed, join as the node is.</summary>
    Skip,

    /// <summary>The seed cannot help (the chain is outside the retention window, or no chain exists): join with an empty store.</summary>
    Fallback
}

/// <summary>
/// The outcome of the base image step of a PITR bootstrap, decided before the persistence backend opens.
/// The replication service reads it to decide whether to replay the chain and seed the WAL.
/// </summary>
public sealed class PitrBootstrapPlan
{
    internal PitrBootstrapPlan(PitrBootstrapAction action, Guid? leafBackupId, string storagePath, bool resumed, string reason)
    {
        Action = action;
        LeafBackupId = leafBackupId;
        StoragePath = storagePath;
        Resumed = resumed;
        Reason = reason;
    }

    public PitrBootstrapAction Action { get; }

    /// <summary>The leaf of the chain the seed uses. Null when no chain was resolved.</summary>
    public Guid? LeafBackupId { get; }

    /// <summary>The storage path the base image was published into, which also holds the seed marker.</summary>
    public string StoragePath { get; }

    /// <summary>True when the base image was published by an earlier start whose seed did not complete.</summary>
    public bool Resumed { get; }

    /// <summary>Why the step decided as it did, for the startup log.</summary>
    public string Reason { get; }
}

/// <summary>Progress of a seed, recorded in the marker next to the store.</summary>
internal enum BootstrapSeedState
{
    /// <summary>The verified base image is moving into place. A start that finds this state removes what the move published.</summary>
    Publishing,

    /// <summary>The base image is in place, but the chain replay and the WAL seed have not completed.</summary>
    Materialized,

    /// <summary>The chain replay and the WAL seed completed. The node never seeds again.</summary>
    Seeded
}

/// <summary>
/// The marker a seed keeps in the storage path. It is the only proof that the data in the store came from a
/// seed and not from a node that ran, so it is what lets a restart continue an incomplete seed, or remove a
/// half-published base image, without the risk of touching live data.
/// </summary>
internal sealed class BootstrapSeedMarker
{
    [JsonConverter(typeof(JsonStringEnumConverter<BootstrapSeedState>))]
    public BootstrapSeedState State { get; set; }

    public Guid LeafBackupId { get; set; }

    public Guid FullBackupId { get; set; }

    public string Storage { get; set; } = "";

    public string StorageRevision { get; set; } = "";

    /// <summary>The top-level names the publish moves into the storage path. A rollback deletes these names and no others.</summary>
    public List<string> Entries { get; set; } = [];
}

/// <summary>Inputs of <see cref="BootstrapBaseImage.PrepareAsync"/>.</summary>
/// <param name="LeafBackupId">The leaf to seed from, or null to pick the newest valid chain of this cluster.</param>
/// <param name="FallbackToJoin">
/// When true, a chain outside the retention window (or, with no leaf given, no chain at all) gives
/// <see cref="PitrBootstrapAction.Fallback"/> instead of a failure. Every other refusal stays a failure.
/// </param>
internal sealed record BootstrapBaseImageRequest(
    string Storage,
    string StoragePath,
    string StorageRevision,
    BackupStoragePair BackupStorage,
    Guid? LeafBackupId,
    HLCTimestamp TargetTime,
    TimeSpan PitrWindow,
    TimeSpan BaseSnapshotInterval,
    DateTime NowUtc,
    byte[]? ManifestMacKey,
    string? ClusterId,
    bool FallbackToJoin,
    long ThrottleBytesPerSec,
    StorageProviders? StorageProviders = null);

/// <summary>
/// Puts the full backup's checkpoint into the node's storage path before the persistence backend opens, the
/// first step of a PITR bootstrap. The chain replay and the WAL seed (<see cref="BootstrapHelper"/>) run on
/// the opened backend and replay only the incrementals, so without this step they run over an empty store and
/// the node presents at an index whose data it does not hold.
///
/// <para><b>Layout.</b> The base image lands where the backend opens: RocksDB opens
/// <c>{storagePath}/{revision}/</c>, SQLite opens its files directly in <c>{storagePath}/</c>. This is the
/// same rule as the offline restore.</para>
///
/// <para><b>Staging and publish.</b> The checkpoint is copied into a staging folder inside the storage path
/// (the same filesystem, so a rename is atomic even when the storage path is a mount point) and verified
/// file by file against the full manifest before anything is published. The publish writes a marker that
/// lists the names it moves, then moves them, then marks the marker <see cref="BootstrapSeedState.Materialized"/>.
/// A failure or a crash at any point leaves either no backend data, or a marker that lets the next start
/// remove exactly what was published. A retry therefore starts clean.</para>
///
/// <para><b>Restart rules.</b>
/// <list type="bullet">
///   <item>No marker and no backend data: seed.</item>
///   <item>No marker and backend data: skip. The store belongs to a node that ran; it is never overwritten.</item>
///   <item>Marker <see cref="BootstrapSeedState.Publishing"/>: remove what the publish moved, then seed again.</item>
///   <item>Marker <see cref="BootstrapSeedState.Materialized"/>: continue the seed from the recorded leaf. The
///         replay and the WAL seed are idempotent. A different leaf is refused, because the WAL can already
///         hold part of the seed of the recorded one.</item>
///   <item>Marker <see cref="BootstrapSeedState.Seeded"/>: skip. A node that seeded never seeds again.</item>
/// </list>
/// The caller can therefore always pass the flag on a node, and the node seeds only when its store is empty.</para>
/// </summary>
internal static class BootstrapBaseImage
{
    internal const string MarkerFileName = "kahuna-pitr-seed.json";

    internal const string StagingDirectoryName = ".kahuna-pitr-seed-staging";

    private const string MarkerTempSuffix = ".tmp";

    private const string PayloadDirectoryName = "payload";

    internal static async Task<PitrBootstrapPlan> PrepareAsync(BootstrapBaseImageRequest request, CancellationToken ct = default)
    {
        IPersistenceBackendProvider? provider = (request.StorageProviders ?? StorageProviders.CreateDefault()).FindBackend(request.Storage);
        if (provider is not { KeepsDataOnDisk: true })
            throw new BackupDriverException(
                $"A PITR bootstrap needs a storage backend that keeps its data on disk; the node " +
                $"is configured with '{request.Storage}'.");

        if (string.IsNullOrWhiteSpace(request.StoragePath))
            throw new BackupDriverException("A PITR bootstrap needs a storage path to put the base image into.");

        string root = Path.GetFullPath(request.StoragePath);
        Directory.CreateDirectory(root);

        File.Delete(Path.Combine(root, MarkerFileName + MarkerTempSuffix));

        BootstrapSeedMarker? marker = ReadMarker(root);
        if (marker is not null)
        {
            switch (marker.State)
            {
                case BootstrapSeedState.Seeded:
                    return new(PitrBootstrapAction.Skip, marker.LeafBackupId, root, resumed: false,
                        $"the store was already seeded from backup {marker.LeafBackupId:N}");

                case BootstrapSeedState.Materialized:
                    if (request.LeafBackupId is { } requested && requested != marker.LeafBackupId)
                        throw new BackupDriverException(
                            $"The storage path holds the base image of backup {marker.LeafBackupId:N} from a seed that did " +
                            $"not complete, but the bootstrap asks for backup {requested:N}. The WAL can already hold part " +
                            "of the first seed, so the node cannot switch chains: restart with the first backup id, or " +
                            "empty both the storage path and the WAL path.") { TargetConflict = true };

                    if (!string.Equals(marker.Storage, request.Storage, StringComparison.Ordinal) ||
                        !string.Equals(marker.StorageRevision, request.StorageRevision, StringComparison.Ordinal))
                        throw new BackupDriverException(
                            $"The storage path holds a base image published for storage '{marker.Storage}' revision " +
                            $"'{marker.StorageRevision}', but the node is configured with '{request.Storage}' revision " +
                            $"'{request.StorageRevision}'.") { TargetConflict = true };

                    return new(PitrBootstrapAction.Seed, marker.LeafBackupId, root, resumed: true,
                        $"continuing the seed from backup {marker.LeafBackupId:N}, whose base image an earlier start published");

                case BootstrapSeedState.Publishing:
                    RollBackPublish(root, marker);
                    break;

                default:
                    throw new BackupDriverException($"The seed marker in the storage path has an unknown state '{marker.State}'.");
            }
        }

        // The staging folder carries a fixed name inside the storage path, so anything there is debris of a
        // start that stopped before its publish.
        DeleteDirectoryIfExists(Path.Combine(root, StagingDirectoryName));

        if (provider.HasData(root, request.StorageRevision))
            return new(PitrBootstrapAction.Skip, null, root, resumed: false,
                "the store already holds data, so it belongs to a node that ran; it is never overwritten");

        BackupCatalog catalog = new(request.BackupStorage.Manifests);

        IReadOnlyList<BackupManifest>? chain;
        if (request.LeafBackupId is { } leafId)
        {
            chain = await catalog.ResolveAndValidateAsync(leafId, ct).ConfigureAwait(false);
        }
        else
        {
            chain = await ResolveLatestChainAsync(catalog, request, ct).ConfigureAwait(false);
            if (chain is null)
            {
                string message = string.IsNullOrWhiteSpace(request.ClusterId)
                    ? "no valid backup chain exists in the catalog"
                    : $"no valid backup chain of cluster '{request.ClusterId}' exists in the catalog";

                if (request.FallbackToJoin)
                    return new(PitrBootstrapAction.Fallback, null, root, resumed: false, message);

                throw new BackupChainException(
                    $"The bootstrap asks for the newest backup, but {message}. Take a backup first, or start the node " +
                    "without the bootstrap so it joins through the normal catch-up.");
            }
        }

        BackupManifest leaf = chain[^1];

        HLCTimestamp targetTime = BootstrapHelper.ValidateChainForSeed(chain, request.TargetTime, request.ManifestMacKey, request.ClusterId);

        try
        {
            BootstrapHelper.EnsureWithinPitrWindow(targetTime, request.PitrWindow, request.NowUtc, request.BaseSnapshotInterval);
        }
        catch (BackupDriverException ex) when (ex.OutsidePitrWindow && request.FallbackToJoin)
        {
            return new(PitrBootstrapAction.Fallback, leaf.BackupId, root, resumed: false, ex.Message);
        }

        BackupManifest full = chain[0];
        EnsureStorageMatches(full, request.Storage, request.StorageRevision);

        string staging = Path.Combine(root, StagingDirectoryName);
        string payload = Path.Combine(staging, PayloadDirectoryName);

        try
        {
            BackupFilePermissions.CreateDirectory(staging);
            Directory.CreateDirectory(payload);

            // The backend can open only a filesystem path, so the checkpoint comes to local disk whether the
            // store is a local folder or a bucket.
            await request.BackupStorage.Artifacts.MaterializeAsync(
                full.BackupId,
                LocalDirectoryArtifactStore.CheckpointDirectoryName,
                payload,
                request.ThrottleBytesPerSec,
                ct).ConfigureAwait(false);

            // The bytes the backend opens are the bytes that pass here: a truncated or corrupt transfer, or a
            // source object that changed, stops the seed before anything is published.
            BackupArtifactVerifier.VerifyCheckpointCopy(full, payload, ct);

            ct.ThrowIfCancellationRequested();

            Publish(root, payload, provider, new BootstrapSeedMarker
            {
                State = BootstrapSeedState.Publishing,
                LeafBackupId = leaf.BackupId,
                FullBackupId = full.BackupId,
                Storage = request.Storage,
                StorageRevision = request.StorageRevision
            });
        }
        finally
        {
            DeleteDirectoryIfExists(staging);
        }

        return new(PitrBootstrapAction.Seed, leaf.BackupId, root, resumed: false,
            $"published the base image of full backup {full.BackupId:N} for the chain ending at {leaf.BackupId:N}");
    }

    /// <summary>
    /// Records that the chain replay and the WAL seed completed, so a later start never seeds again. Call it
    /// only after the seed returned successfully.
    /// </summary>
    internal static void MarkSeeded(string storagePath)
    {
        string root = Path.GetFullPath(storagePath);
        BootstrapSeedMarker marker = ReadMarker(root)
            ?? throw new BackupDriverException("The seed marker is missing from the storage path; the seed cannot be recorded as complete.");

        if (marker.State == BootstrapSeedState.Seeded)
            return;

        if (marker.State != BootstrapSeedState.Materialized)
            throw new BackupDriverException($"The seed marker is in state '{marker.State}'; only a published base image can complete a seed.");

        marker.State = BootstrapSeedState.Seeded;
        WriteMarker(root, marker);
    }

    /// <summary>
    /// The newest chain that passes every seed check except the retention window. The window is left to the
    /// caller, so a newest chain that is too old gives the window refusal (or fallback) and not an older chain.
    /// Candidates are ordered by the newest commit HLC their leaf covers.
    /// </summary>
    private static async Task<IReadOnlyList<BackupManifest>?> ResolveLatestChainAsync(
        BackupCatalog catalog, BootstrapBaseImageRequest request, CancellationToken ct)
    {
        IReadOnlyList<BackupManifest> manifests = await catalog.ListAsync(ct).ConfigureAwait(false);

        List<BackupManifest> candidates = new(manifests.Count);
        foreach (BackupManifest manifest in manifests)
        {
            if (!string.IsNullOrWhiteSpace(request.ClusterId) &&
                !string.Equals(manifest.ClusterId, request.ClusterId, StringComparison.Ordinal))
                continue;

            if (BackupArtifactVerifier.GetStructuralError(manifest) is not null)
                continue;

            candidates.Add(manifest);
        }

        candidates.Sort(static (a, b) =>
        {
            int byHlc = MaxCoveredHlc(b).CompareTo(MaxCoveredHlc(a));
            return byHlc != 0 ? byHlc : b.CreatedAtUtc.CompareTo(a.CreatedAtUtc);
        });

        foreach (BackupManifest candidate in candidates)
        {
            ct.ThrowIfCancellationRequested();

            IReadOnlyList<BackupManifest> chain;
            try
            {
                chain = await catalog.ResolveAndValidateAsync(candidate.BackupId, ct).ConfigureAwait(false);

                BootstrapHelper.ValidateChainForSeed(chain, request.TargetTime, request.ManifestMacKey, request.ClusterId);

                EnsureStorageMatches(chain[0], request.Storage, request.StorageRevision);
            }
            catch (Exception ex) when (ex is BackupChainException or BackupArtifactException or BackupDriverException)
            {
                continue;
            }

            return chain;
        }

        return null;
    }

    private static HLCTimestamp MaxCoveredHlc(BackupManifest manifest)
    {
        HLCTimestamp max = HLCTimestamp.Zero;
        foreach (PartitionBackupRange range in manifest.PartitionRanges)
        {
            if (range.ToHlc.CompareTo(max) > 0)
                max = range.ToHlc;
        }
        return max;
    }

    /// <summary>
    /// Refuses a base image that the configured backend cannot open as this node's store. Both fields are
    /// optional on a manifest; an unknown value is accepted, as the chain validator accepts it.
    /// </summary>
    private static void EnsureStorageMatches(BackupManifest full, string storage, string storageRevision)
    {
        if (full.StorageType is not null && !string.Equals(full.StorageType, storage, StringComparison.Ordinal))
            throw new BackupDriverException(
                $"Full backup {full.BackupId:N} holds a '{full.StorageType}' checkpoint, but the node is configured with " +
                $"'{storage}' storage; the backend cannot open it.");

        // SQLite names its files by revision, so a checkpoint of another revision opens as an empty store. The
        // revision is also the format marker the chain validator gates on, so it must match for RocksDB as well.
        if (full.StorageRevision is not null && !string.Equals(full.StorageRevision, storageRevision, StringComparison.Ordinal))
            throw new BackupDriverException(
                $"Full backup {full.BackupId:N} was written with storage revision '{full.StorageRevision}', but the node " +
                $"is configured with revision '{storageRevision}'. Start the node with --storage-revision {full.StorageRevision}.");
    }

    /// <summary>
    /// True when the files the backend opens already exist. Other content of the storage path (a filesystem's
    /// <c>lost+found</c>, the data of another revision) is not this store and does not stop a seed.
    /// </summary>
    internal static bool HasBackendData(string root, string storage, string storageRevision, StorageProviders? providers = null) =>
        (providers ?? StorageProviders.CreateDefault()).GetBackend(storage).HasData(root, storageRevision);

    /// <summary>
    /// Moves the verified payload into the storage path under the protection of the marker. The marker names
    /// every entry before the first move, so a crash during the moves leaves enough to remove them.
    /// </summary>
    private static void Publish(string root, string payload, IPersistenceBackendProvider provider, BootstrapSeedMarker marker)
    {
        List<(string Source, string Target, bool IsDirectory)> moves = [];

        string dataDirectory = Path.GetFullPath(provider.DataDirectory(root, marker.StorageRevision));
        if (!string.Equals(dataDirectory, root, StringComparison.Ordinal))
        {
            // A store in its own directory under the root: the rename is the publish.
            moves.Add((payload, dataDirectory, true));
            marker.Entries.Add(Path.GetRelativePath(root, dataDirectory));
        }
        else
        {
            foreach (string entry in Directory.EnumerateFileSystemEntries(payload))
            {
                string name = Path.GetFileName(entry);
                bool isDirectory = Directory.Exists(entry);
                moves.Add((entry, Path.Combine(root, name), isDirectory));
                marker.Entries.Add(name);
            }
        }

        // A name that is already in the storage path is not ours to replace, and a rollback must never
        // delete it. Refuse before the marker claims it.
        foreach ((_, string target, _) in moves)
        {
            if (File.Exists(target) || Directory.Exists(target))
                throw new BackupDriverException(
                    $"The storage path already holds '{Path.GetFileName(target)}', which the base image would replace. " +
                    "Remove it, or start the node on an empty storage path.") { TargetConflict = true };
        }

        WriteMarker(root, marker);

        try
        {
            foreach ((string source, string target, bool isDirectory) in moves)
            {
                if (isDirectory)
                    Directory.Move(source, target);
                else
                    File.Move(source, target);
            }

            marker.State = BootstrapSeedState.Materialized;
            WriteMarker(root, marker);
        }
        catch
        {
            // Leave the storage path empty now, so a retry starts clean. When this cleanup also fails, the
            // marker stays in its publishing state and the next start completes the cleanup.
            try
            {
                marker.State = BootstrapSeedState.Publishing;
                RollBackPublish(root, marker);
            }
            catch
            {
                // The next start rolls back from the marker on disk.
            }

            throw;
        }
    }

    /// <summary>
    /// Removes what an interrupted publish moved into the storage path, then the marker. Only the names the
    /// marker lists are touched: they did not exist before the publish, so they hold nothing but the seed.
    /// </summary>
    private static void RollBackPublish(string root, BootstrapSeedMarker marker)
    {
        foreach (string name in marker.Entries)
        {
            if (string.IsNullOrEmpty(name) || name is "." or ".." ||
                name.IndexOfAny([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar]) >= 0)
                throw new BackupDriverException(
                    $"The seed marker names an entry '{name}' that is not a plain name in the storage path; it was not " +
                    "written by a seed. Inspect the storage path before the node starts.");

            string path = Path.Combine(root, name);
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
            else if (File.Exists(path))
                File.Delete(path);
        }

        File.Delete(Path.Combine(root, MarkerFileName));
    }

    private static BootstrapSeedMarker? ReadMarker(string root)
    {
        string path = Path.Combine(root, MarkerFileName);
        if (!File.Exists(path))
            return null;

        try
        {
            return JsonSerializer.Deserialize(File.ReadAllText(path), PitrIndentedJsonContext.Default.BootstrapSeedMarker)
                   ?? throw new BackupDriverException("The seed marker in the storage path is empty.");
        }
        catch (JsonException ex)
        {
            throw new BackupDriverException(
                $"The seed marker in the storage path cannot be read ({ex.Message}). Inspect the storage path before the " +
                "node starts; it is not removed automatically because it is the proof of what the seed published.");
        }
    }

    /// <summary>Replaces the marker atomically: a durable temporary file, then a rename over the old marker.</summary>
    private static void WriteMarker(string root, BootstrapSeedMarker marker)
    {
        string path = Path.Combine(root, MarkerFileName);
        string temp = path + MarkerTempSuffix;

        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(marker, PitrIndentedJsonContext.Default.BootstrapSeedMarker);
        using (FileStream stream = new(temp, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
        {
            stream.Write(bytes);
            stream.Flush(flushToDisk: true);
        }

        BackupFilePermissions.RestrictFile(temp);
        File.Move(temp, path, overwrite: true);
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}

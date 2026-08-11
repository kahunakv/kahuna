
using System.Collections.Concurrent;
using System.Text.Json;
using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Server.Tests;

/// <summary>
/// An in-memory stand-in for an object store, modelling the semantics that differ from a filesystem and
/// that the local implementation therefore cannot exercise:
/// <list type="bullet">
///   <item>no rename — an object becomes visible only when its write is completed;</item>
///   <item>no atomic prefix delete — deletion is a listing plus per-object deletes that can fail
///         part-way, leaving a partially deleted backup;</item>
///   <item>paged listing — keys are returned in pages internally, so a caller that assumed one
///         response covered a prefix would see a short listing;</item>
///   <item>one flat keyspace shared by manifests and artifacts, as a single bucket prefix would be;</item>
///   <item>no POSIX permissions, and a mandatory local scratch area for checkpoints.</item>
/// </list>
/// <para>
/// Faults are injectable so the failure paths — mid-upload, mid-batch-delete, unreadable listing — can be
/// driven deterministically rather than hoped for.
/// </para>
/// </summary>
internal sealed class FakeObjectStore
{
    /// <summary>The bucket: one flat keyspace holding both manifest and artifact objects.</summary>
    private readonly ConcurrentDictionary<string, byte[]> _objects = new(StringComparer.Ordinal);

    /// <summary>Keys returned per listing page, mirroring a real store's pagination.</summary>
    internal int PageSize { get; set; } = 2;

    /// <summary>When set, the next delete whose ordinal matches throws. 1 = the very next delete.</summary>
    internal int? FailDeleteNumber { get; set; }

    /// <summary>When set, an upload throws after this many bytes have been buffered.</summary>
    internal int? FailUploadAfterBytes { get; set; }

    /// <summary>
    /// When set, the object upload whose ordinal matches throws. 1 = the first object of the next
    /// commit. Unlike <see cref="FailUploadAfterBytes"/> this is deterministic regardless of file sizes,
    /// so a test can land some objects and fail on a later one — the partial-upload state that matters.
    /// </summary>
    internal int? FailUploadNumber { get; set; }

    /// <summary>When true, every listing throws — a transient outage the caller must not read as "empty".</summary>
    internal bool FailListings { get; set; }

    private int _deleteCount;
    private int _uploadCount;

    internal IReadOnlyCollection<string> Keys => _objects.Keys.ToList();

    internal int CountUnder(string prefix) =>
        _objects.Keys.Count(k => k.StartsWith(prefix, StringComparison.Ordinal));

    internal void Put(string key, byte[] value) => _objects[key] = value;

    internal bool TryGet(string key, out byte[] value) => _objects.TryGetValue(key, out value!);

    internal bool Exists(string key) => _objects.ContainsKey(key);

    /// <summary>
    /// Lists keys under a prefix, one page at a time, so a paging bug in a caller shows up as missing
    /// keys rather than passing by accident.
    /// </summary>
    internal List<string> List(string prefix)
    {
        if (FailListings)
            throw new IOException("Simulated listing failure (transient).");

        List<string> all = _objects.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        List<string> paged = [];
        for (int offset = 0; offset < all.Count; offset += PageSize)
            paged.AddRange(all.Skip(offset).Take(PageSize));
        return paged;
    }

    /// <summary>Deletes one object, honouring the injected delete fault. Absent keys are a no-op.</summary>
    internal void Delete(string key)
    {
        _deleteCount++;
        if (FailDeleteNumber == _deleteCount)
            throw new IOException($"Simulated delete failure on delete #{_deleteCount} ('{key}').");

        _objects.TryRemove(key, out _);
    }

    internal void CheckUploadFault(long bytesSoFar)
    {
        if (FailUploadAfterBytes is { } limit && bytesSoFar > limit)
            throw new IOException($"Simulated upload failure after {bytesSoFar} bytes.");
    }

    /// <summary>Counts one object upload and throws when it is the one the test asked to fail.</summary>
    internal void CheckObjectUploadFault()
    {
        _uploadCount++;
        if (FailUploadNumber == _uploadCount)
            throw new IOException($"Simulated upload failure on object #{_uploadCount}.");
    }

    internal void ResetFaults()
    {
        FailDeleteNumber = null;
        FailUploadAfterBytes = null;
        FailUploadNumber = null;
        FailListings = false;
        _deleteCount = 0;
        _uploadCount = 0;
    }

    // ── key layout ────────────────────────────────────────────────────────────────────────────
    //
    // Manifests and artifacts share one keyspace, as they share one directory today. The sweep has to
    // tell them apart by name, so the fake keeps that ambiguity rather than hiding it behind two maps.

    internal const string ManifestSuffix = ".manifest";

    internal static string ManifestKey(Guid backupId) => backupId.ToString("N") + ManifestSuffix;

    internal static string ArtifactKey(Guid backupId, string relativePath) =>
        backupId.ToString("N") + "/" + relativePath;

    internal static string ArtifactPrefix(Guid backupId) => backupId.ToString("N") + "/";
}

/// <summary>Manifest target over a <see cref="FakeObjectStore"/>.</summary>
internal sealed class FakeObjectManifestTarget(FakeObjectStore store) : IBackupStorageTarget
{
    public Task PutAsync(BackupManifest manifest, CancellationToken ct = default)
    {
        store.Put(FakeObjectStore.ManifestKey(manifest.BackupId), JsonSerializer.SerializeToUtf8Bytes(manifest));
        return Task.CompletedTask;
    }

    public Task DeleteAsync(Guid backupId, CancellationToken ct = default)
    {
        store.Delete(FakeObjectStore.ManifestKey(backupId));
        return Task.CompletedTask;
    }

    public Task<BackupManifest?> GetAsync(Guid backupId, CancellationToken ct = default) =>
        Task.FromResult(store.TryGet(FakeObjectStore.ManifestKey(backupId), out byte[] bytes)
            ? JsonSerializer.Deserialize<BackupManifest>(bytes)
            : null);

    public Task<IReadOnlyList<BackupManifest>> ListAsync(CancellationToken ct = default)
    {
        List<BackupManifest> result = [];
        foreach (string key in store.List(""))
        {
            if (!key.EndsWith(FakeObjectStore.ManifestSuffix, StringComparison.Ordinal))
                continue;
            if (!store.TryGet(key, out byte[] bytes))
                continue;
            try
            {
                BackupManifest? m = JsonSerializer.Deserialize<BackupManifest>(bytes);
                if (m is not null)
                    result.Add(m);
            }
            catch (JsonException)
            {
                // Reported by ListCorruptAsync instead, so one bad object cannot blind the listing.
            }
        }
        return Task.FromResult<IReadOnlyList<BackupManifest>>(result);
    }

    public Task<IReadOnlyList<(Guid backupId, string reason)>> ListCorruptAsync(CancellationToken ct = default)
    {
        List<(Guid, string)> corrupt = [];
        foreach (string key in store.List(""))
        {
            if (!key.EndsWith(FakeObjectStore.ManifestSuffix, StringComparison.Ordinal))
                continue;
            if (!store.TryGet(key, out byte[] bytes))
                continue;
            try
            {
                if (JsonSerializer.Deserialize<BackupManifest>(bytes) is null)
                    corrupt.Add((ParseId(key), "Manifest deserialized to null."));
            }
            catch (JsonException ex)
            {
                corrupt.Add((ParseId(key), $"Manifest is not valid JSON: {ex.Message}"));
            }
        }
        return Task.FromResult<IReadOnlyList<(Guid, string)>>(corrupt);
    }

    public Task<IReadOnlyList<Guid>> ListManifestIdsAsync(CancellationToken ct = default)
    {
        List<Guid> ids = [];
        foreach (string key in store.List(""))
        {
            if (!key.EndsWith(FakeObjectStore.ManifestSuffix, StringComparison.Ordinal))
                continue;
            Guid id = ParseId(key);
            if (id != Guid.Empty)
                ids.Add(id);
        }
        return Task.FromResult<IReadOnlyList<Guid>>(ids);
    }

    private static Guid ParseId(string key)
    {
        string name = key[..^FakeObjectStore.ManifestSuffix.Length];
        return Guid.TryParseExact(name, "N", out Guid id) ? id : Guid.Empty;
    }
}

/// <summary>
/// Artifact store over a <see cref="FakeObjectStore"/>. Declares no POSIX hardening and a mandatory
/// local scratch area, so exercising it drives the staging-then-upload path a bucket target uses — the
/// path the local store deliberately skips.
/// </summary>
internal sealed class FakeObjectArtifactStore(FakeObjectStore store, string scratchRoot) : IBackupArtifactStore
{
    public BackupArtifactStoreCapabilities Capabilities { get; } = new(
        SupportsPosixHardening: false,
        SupportsCheapRangeReads: true,
        RequiresLocalScratch: true);

    public Task<bool> BackupExistsAsync(Guid backupId, CancellationToken ct = default) =>
        Task.FromResult(store.List(FakeObjectStore.ArtifactPrefix(backupId)).Count > 0);

    public Task<bool> ExistsAsync(Guid backupId, string relativePath, CancellationToken ct = default) =>
        Task.FromResult(store.Exists(FakeObjectStore.ArtifactKey(backupId, relativePath)));

    public Task<IReadOnlyList<BackupArtifactEntry>> ListAsync(Guid backupId, CancellationToken ct = default)
    {
        string prefix = FakeObjectStore.ArtifactPrefix(backupId);
        List<BackupArtifactEntry> entries = [];
        foreach (string key in store.List(prefix))
        {
            if (!store.TryGet(key, out byte[] bytes))
                continue;
            entries.Add(new BackupArtifactEntry(key[prefix.Length..], bytes.LongLength));
        }
        entries.Sort(static (a, b) => string.CompareOrdinal(a.RelativePath, b.RelativePath));
        return Task.FromResult<IReadOnlyList<BackupArtifactEntry>>(entries);
    }

    public Task<Stream> OpenReadAsync(
        Guid backupId, string relativePath, long offset = 0, long? length = null, CancellationToken ct = default)
    {
        if (!store.TryGet(FakeObjectStore.ArtifactKey(backupId, relativePath), out byte[] bytes))
            throw new BackupArtifactException($"Backup {backupId:N}: artifact '{relativePath}' not found.");

        int start = (int)offset;
        int count = (int)Math.Min(length ?? bytes.Length - start, bytes.Length - start);
        return Task.FromResult<Stream>(new MemoryStream(bytes, start, count, writable: false));
    }

    public Task<IBackupArtifactWriter> OpenWriteAsync(
        Guid backupId, string relativePath, CancellationToken ct = default) =>
        Task.FromResult<IBackupArtifactWriter>(
            new FakeUpload(store, FakeObjectStore.ArtifactKey(backupId, relativePath)));

    public Task<IReadOnlyList<Guid>> ListBackupIdsAsync(CancellationToken ct = default)
    {
        HashSet<Guid> ids = [];
        foreach (string key in store.List(""))
        {
            int slash = key.IndexOf('/');
            if (slash <= 0)
                continue;
            if (Guid.TryParseExact(key[..slash], "N", out Guid id))
                ids.Add(id);
        }
        return Task.FromResult<IReadOnlyList<Guid>>(ids.ToList());
    }

    /// <summary>
    /// A single-shot upload either lands whole or not at all, so this store has no partial-object debris
    /// class and reports none. (A real multipart upload does, but its abandoned parts are reclaimed by a
    /// bucket lifecycle rule rather than by Kahuna.)
    /// </summary>
    public Task<IReadOnlyList<BackupArtifactLeftover>> ListLeftoversAsync(CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<BackupArtifactLeftover>>([]);

    public Task DeleteLeftoverAsync(BackupArtifactLeftover leftover, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task DeleteAllAsync(Guid backupId, CancellationToken ct = default)
    {
        // Non-atomic on purpose: list the prefix, then delete key by key. An injected failure part-way
        // leaves the rest behind, which is exactly the state a re-run has to converge from.
        foreach (string key in store.List(FakeObjectStore.ArtifactPrefix(backupId)))
        {
            ct.ThrowIfCancellationRequested();
            store.Delete(key);
        }
        return Task.CompletedTask;
    }

    public Task<IBackupCheckpointStagingArea> BeginCheckpointAsync(Guid backupId, CancellationToken ct = default)
    {
        // Scratch parent exists; the leaf must not, because the backend renames a sibling temp tree onto it.
        string parent = Path.Combine(scratchRoot, backupId.ToString("N"));
        Directory.CreateDirectory(parent);
        string localPath = Path.Combine(parent, "checkpoint");
        return Task.FromResult<IBackupCheckpointStagingArea>(new ScratchStagingArea(store, backupId, localPath));
    }

    public Task MaterializeAsync(
        Guid backupId,
        string relativePrefix,
        string destinationDirectory,
        long throttleBytesPerSec = 0,
        CancellationToken ct = default)
    {
        string prefix = FakeObjectStore.ArtifactPrefix(backupId)
                        + (string.IsNullOrEmpty(relativePrefix) ? "" : relativePrefix.TrimEnd('/') + "/");

        List<string> keys = store.List(prefix);
        if (keys.Count == 0)
            throw new BackupArtifactException(
                $"Backup {backupId:N}: artifacts under '{relativePrefix}' are missing.");

        foreach (string key in keys)
        {
            ct.ThrowIfCancellationRequested();
            if (!store.TryGet(key, out byte[] bytes))
                continue;

            // Relative paths are preserved exactly — a backend opens this tree and is layout-sensitive.
            string relative = key[prefix.Length..].Replace('/', Path.DirectorySeparatorChar);
            string destination = Path.Combine(destinationDirectory, relative);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.WriteAllBytes(destination, bytes);
        }

        return Task.CompletedTask;
    }

    /// <summary>Buffers the upload and publishes it as one object on completion — never before.</summary>
    private sealed class FakeUpload(FakeObjectStore store, string key) : IBackupArtifactWriter
    {
        private readonly FaultingMemoryStream _buffer = new(store);
        private bool _completed;

        public Stream Stream => _buffer;

        public Task CompleteAsync(CancellationToken ct = default)
        {
            if (_completed)
                throw new InvalidOperationException("This artifact write has already been completed.");
            store.Put(key, _buffer.ToArray());
            _completed = true;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            // Nothing to undo: an incomplete upload was never published in the first place.
            _buffer.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>A buffer that can fail mid-write, standing in for a connection dropping mid-upload.</summary>
    private sealed class FaultingMemoryStream(FakeObjectStore store) : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count)
        {
            base.Write(buffer, offset, count);
            store.CheckUploadFault(Length);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            base.Write(buffer);
            store.CheckUploadFault(Length);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            await base.WriteAsync(buffer, ct);
            store.CheckUploadFault(Length);
        }
    }

    /// <summary>
    /// Stages the checkpoint on local disk, then uploads every file on commit. This is the path a real
    /// bucket target takes, and the reason a scratch directory is mandatory for one.
    /// </summary>
    private sealed class ScratchStagingArea(FakeObjectStore store, Guid backupId, string localPath)
        : IBackupCheckpointStagingArea
    {
        public string LocalPath { get; } = localPath;

        public Task CommitAsync(CancellationToken ct = default)
        {
            foreach (string file in Directory.EnumerateFiles(LocalPath, "*", SearchOption.AllDirectories))
            {
                ct.ThrowIfCancellationRequested();
                string relative = Path.GetRelativePath(LocalPath, file).Replace(Path.DirectorySeparatorChar, '/');
                byte[] bytes = File.ReadAllBytes(file);
                store.CheckUploadFault(bytes.LongLength);
                store.CheckObjectUploadFault();
                store.Put(FakeObjectStore.ArtifactKey(backupId, "checkpoint/" + relative), bytes);
            }
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            // The scratch tree is transient whatever the outcome; leaving it would let a later run mistake
            // it for valid input.
            try
            {
                if (Directory.Exists(LocalPath))
                    Directory.Delete(LocalPath, recursive: true);
            }
            catch
            {
                // Best-effort cleanup of a scratch tree.
            }
            return ValueTask.CompletedTask;
        }
    }
}


using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Server.Persistence;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Drives the real backup, restore, and garbage-collection entry points against a store with
/// object-storage semantics — no rename, non-atomic prefix delete, paged listing, one shared keyspace,
/// no POSIX permissions, and a mandatory local scratch area for checkpoints.
/// <para>
/// The existing PITR tests all run on a local directory and would keep passing whether or not these
/// paths work, so this file is where the remote-shaped behavior is actually verified: the staging-then-
/// upload flow, download-before-open on restore, and convergence of reclamation after a failure part-way
/// through a delete that a filesystem would have done atomically.
/// </para>
/// </summary>
public sealed class TestBackupObjectStoreSemantics : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_objstore_" + Guid.NewGuid().ToString("N"));

    private readonly FakeObjectStore _bucket = new();

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    // ── scaffolding ──────────────────────────────────────────────────────────────────────────

    private string ScratchDir => Path.Combine(_tempRoot, "scratch");

    private FakeObjectArtifactStore Artifacts() => new(_bucket, ScratchDir);

    private FakeObjectManifestTarget Manifests() => new(_bucket);

    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs,
        };
        return new RaftLog
        {
            Id = id, Term = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.KeyValues, LogData = ReplicationSerializer.Serialize(msg),
        };
    }

    private static InMemoryWAL BuildWal(params (int partition, RaftLog log)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];
        foreach ((int partition, RaftLog log) in entries)
        {
            if (!byPartition.TryGetValue(partition, out List<RaftLog>? list))
                byPartition[partition] = list = [];
            list.Add(log);
        }
        foreach ((int partition, List<RaftLog> logs) in byPartition)
            wal.Write([(partition, logs)]);
        return wal;
    }

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    /// <summary>Applies a WAL's key-value entries to a backend so the checkpoint has state in it.</summary>
    private static void FlushWalToBackend(InMemoryWAL wal, int partitionId, MemoryPersistenceBackend backend)
    {
        foreach (RaftLog log in wal.ReadLogsRange(partitionId, 1))
        {
            if (log.Type != RaftLogType.Committed || log.LogType != ReplicationTypes.KeyValues || log.LogData is null)
                continue;
            KeyValueMessage msg = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
            (KeyValueState state, byte[]? value) = KeyValueMessageDecoder.Decode(msg);
            if (state == KeyValueState.Undefined) continue;
            backend.StoreKeyValues([new PersistenceRequestItem(
                msg.Key, value, msg.Revision, msg.ExpireNode, msg.ExpirePhysical, msg.ExpireCounter,
                msg.LastUsedNode, msg.LastUsedPhysical, msg.LastUsedCounter,
                msg.LastModifiedNode, msg.LastModifiedPhysical, msg.LastModifiedCounter, (int)state)]);
        }
    }

    private BackupCatalog NewCatalog() => new(Manifests());

    /// <summary>
    /// A service wired to the same bucket the tests drive the driver against, so a listing sees whatever
    /// those lower-level calls published. Its own store instances are separate objects over one shared
    /// <see cref="FakeObjectStore"/> — which is also true of two nodes pointed at one bucket.
    /// </summary>
    private BackupService MakeService(MemoryPersistenceBackend backend, InMemoryWAL wal) =>
        new(new TestBackupService.StubRaft(wal, [Part(1)]),
            backend,
            Path.Combine(_tempRoot, "host"),
            Manifests(),
            Artifacts(),
            storageType: "memory",
            storageRevision: "",
            flushBeforeCheckpoint: () => Task.CompletedTask,
            queryMinInFlight: () => Task.FromResult(HLCTimestamp.Zero));

    // ── round trip through the real entry points ─────────────────────────────────────────────

    [Fact]
    public async Task FullBackup_StagesLocallyThenUploads_AndVerifiesFromTheStore()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        // The checkpoint reached the bucket, keyed under the backup's prefix.
        Assert.True(_bucket.Exists(FakeObjectStore.ArtifactKey(full.BackupId, "checkpoint/store.json")));
        Assert.True(_bucket.Exists(
            FakeObjectStore.ArtifactKey(full.BackupId, "checkpoint/" + CheckpointManifest.FileName)));

        // The manifest is published last, so its presence means the artifacts are already there.
        Assert.NotNull(await catalog.GetAsync(full.BackupId, TestContext.Current.CancellationToken));

        // Verification reads back through the store: the digests recorded at staging time describe the
        // bytes the bucket now serves.
        await BackupArtifactVerifier.VerifyAsync(full, artifacts, TestContext.Current.CancellationToken);

        // Scratch does not survive the backup — a leftover tree could be mistaken for valid input later.
        Assert.False(Directory.Exists(Path.Combine(ScratchDir, full.BackupId.ToString("N"), "checkpoint")));
    }

    [Fact]
    public async Task IncrementalChain_RestoresToPointInTime_FromTheStore()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        // Two later commits, so a restore target can land between them.
        wal.Write([(1, [KvLog(2, 200, "k2", "v2", 1)])]);
        wal.Write([(1, [KvLog(3, 300, "k3", "v3", 1)])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(
            wal, [Part(1)], full.BackupId, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        IReadOnlyList<BackupManifest> chain =
            await catalog.ResolveAndValidateAsync(inc.BackupId, TestContext.Current.CancellationToken);

        // Restore to T=200: the second commit is included, the third is not.
        MemoryPersistenceBackend target = new();
        RestoreResult result = await RestoreEngine.RestoreAsync(
            chain, artifacts, new HLCTimestamp(0, 200, uint.MaxValue), target,
            ct: TestContext.Current.CancellationToken);

        Assert.Equal(1, result.EntriesApplied);
        Assert.NotNull(target.GetKeyValue("k2"));
        Assert.Null(target.GetKeyValue("k3"));
    }

    // ── failure injection ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task UploadFailsMidCheckpoint_NoManifestPublished_AndPartialArtifactsCleanedUp()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();

        // Land the first object, then fail: a genuinely partial upload rather than one that never began.
        _bucket.FailUploadNumber = 2;

        await Assert.ThrowsAnyAsync<IOException>(() => BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken));

        _bucket.ResetFaults();

        // The invariant that matters: no manifest was published, so nothing resolves to the partial
        // artifacts and no restore can be attempted against them.
        Assert.Empty(await catalog.ListAsync(TestContext.Current.CancellationToken));
        Assert.Empty(await catalog.ListManifestIdsAsync(TestContext.Current.CancellationToken));

        // And the driver's own compensating delete removed what did land, so the common case needs no
        // sweep at all. (When that delete also fails — usually the same outage — the artifacts survive
        // as an orphan; the incomplete-listing and sweep tests below cover that.)
        Assert.Empty(await artifacts.ListBackupIdsAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task FailedUpload_IsListedAsIncomplete_AndDisappearsOnceTheManifestLands()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();
        BackupService svc = MakeService(backend, wal);

        // A backup whose upload fails part-way AND whose compensating delete also fails — the realistic
        // pairing, since whatever broke the upload usually breaks the cleanup too — leaves artifacts
        // behind with no manifest.
        _bucket.FailUploadNumber = 2;
        _bucket.FailDeleteNumber = 1;
        await Assert.ThrowsAnyAsync<IOException>(() => BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken));
        _bucket.ResetFaults();

        Assert.NotEmpty(await artifacts.ListBackupIdsAsync(TestContext.Current.CancellationToken));

        // Which is exactly what a listing must surface: an operator gets told the backup failed instead
        // of seeing nothing at all until a GC pass mentions an anonymous orphan.
        IReadOnlyList<KahunaBackupInfo> listed =
            await svc.ListBackupsAsync(ct: TestContext.Current.CancellationToken);

        KahunaBackupInfo entry = Assert.Single(listed);
        Assert.True(entry.IsIncomplete);
        Assert.True(entry.IsInvalid, "an incomplete backup must also read as unusable to older callers");
        Assert.Contains("no manifest", entry.InvalidReason!, StringComparison.OrdinalIgnoreCase);

        // A backup that completes is a normal entry, not an incomplete one — the flag tracks the missing
        // manifest, not merely the presence of artifacts.
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        listed = await svc.ListBackupsAsync(ct: TestContext.Current.CancellationToken);

        KahunaBackupInfo completed = Assert.Single(listed, e => e.BackupId == full.BackupId);
        Assert.False(completed.IsIncomplete);
        Assert.False(completed.IsInvalid);

        // The failed one is still reported, and still incomplete, until GC reclaims it.
        Assert.Single(listed, e => e.IsIncomplete);
    }

    [Fact]
    public async Task IncompleteBackup_DisappearsFromTheListing_AfterGarbageCollection()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();
        BackupService svc = MakeService(backend, wal);

        _bucket.FailUploadNumber = 2;
        _bucket.FailDeleteNumber = 1;
        await Assert.ThrowsAnyAsync<IOException>(() => BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken));
        _bucket.ResetFaults();

        Assert.Single(await svc.ListBackupsAsync(ct: TestContext.Current.CancellationToken), e => e.IsIncomplete);

        await svc.RunGarbageCollectionAsync(TestContext.Current.CancellationToken);

        Assert.Empty(await svc.ListBackupsAsync(ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DeleteInterruptedMidBatch_LeavesTombstonedBackup_AndConvergesOnRetry()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        int artifactCount = _bucket.CountUnder(FakeObjectStore.ArtifactPrefix(full.BackupId));
        Assert.True(artifactCount > 1, "need several artifacts for a mid-batch failure to be meaningful");

        // Delete #1 is the manifest tombstone; fail on #2, the first artifact object.
        _bucket.FailDeleteNumber = 2;

        await Assert.ThrowsAnyAsync<IOException>(() =>
            catalog.DeleteAsync(full.BackupId, artifacts, TestContext.Current.CancellationToken));

        _bucket.ResetFaults();

        // Manifest-first ordering held: the backup is tombstoned even though its bytes are still partly
        // present, so nothing resolves to a half-deleted artifact set.
        Assert.Null(await catalog.GetAsync(full.BackupId, TestContext.Current.CancellationToken));
        Assert.True(_bucket.CountUnder(FakeObjectStore.ArtifactPrefix(full.BackupId)) > 0);

        // Re-running finishes the job rather than throwing or skipping — the convergence property that
        // replaces atomic directory delete.
        await catalog.DeleteAsync(full.BackupId, artifacts, TestContext.Current.CancellationToken);
        Assert.Equal(0, _bucket.CountUnder(FakeObjectStore.ArtifactPrefix(full.BackupId)));
    }

    [Fact]
    public async Task SweepAfterTombstoneOnly_ReclaimsOrphanedArtifacts()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();
        FakeObjectManifestTarget manifests = Manifests();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        // Crash exactly between the tombstone and the artifact deletes.
        await manifests.DeleteAsync(full.BackupId, TestContext.Current.CancellationToken);

        IReadOnlyList<OrphanSweepCandidate> plan = await BackupRetention.PlanOrphanSweepAsync(
            artifacts, new HashSet<Guid>(), new HashSet<Guid>(), TestContext.Current.CancellationToken);

        OrphanSweepCandidate candidate = Assert.Single(plan);
        Assert.Equal(full.BackupId, candidate.OrphanedBackupId);

        await BackupRetention.ApplyOrphanSweepAsync(plan, artifacts, TestContext.Current.CancellationToken);
        Assert.Equal(0, _bucket.CountUnder(FakeObjectStore.ArtifactPrefix(full.BackupId)));
    }

    [Fact]
    public async Task ListingUnavailable_SweepFailsClosed_ReclaimsNothing()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        int before = _bucket.CountUnder(FakeObjectStore.ArtifactPrefix(full.BackupId));

        // A transient outage must propagate. Read as "the bucket is empty, so nothing is owned", it would
        // reclaim every live backup — the worst possible failure mode for this code path.
        _bucket.FailListings = true;

        await Assert.ThrowsAnyAsync<IOException>(() => BackupRetention.PlanOrphanSweepAsync(
            artifacts, new HashSet<Guid>(), new HashSet<Guid>(), TestContext.Current.CancellationToken));

        _bucket.FailListings = false;
        Assert.Equal(before, _bucket.CountUnder(FakeObjectStore.ArtifactPrefix(full.BackupId)));
    }

    [Fact]
    public async Task OrphanSweep_NeverReclaimsBackupWithAManifest_EvenAnUnreadableOne()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        // Corrupt the manifest object: it no longer parses, so it is absent from the valid-id set, but its
        // presence still proves the backup is owned. Reclaiming it would destroy the very artifacts an
        // operator needs to diagnose the corruption.
        _bucket.Put(FakeObjectStore.ManifestKey(full.BackupId), Encoding.UTF8.GetBytes("{ not json"));

        HashSet<Guid> valid = (await catalog.ListAsync(TestContext.Current.CancellationToken))
            .Select(m => m.BackupId).ToHashSet();
        HashSet<Guid> owned = (await catalog.ListManifestIdsAsync(TestContext.Current.CancellationToken))
            .ToHashSet();

        Assert.DoesNotContain(full.BackupId, valid);
        Assert.Contains(full.BackupId, owned);

        IReadOnlyList<OrphanSweepCandidate> plan = await BackupRetention.PlanOrphanSweepAsync(
            artifacts, valid, owned, TestContext.Current.CancellationToken);

        Assert.Empty(plan);
    }

    [Fact]
    public async Task PagedListing_SeesEveryArtifact_WhenPrefixSpansManyPages()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k1", "v1", 1)));
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        // One key per page, so a caller that read only the first response would see a single artifact.
        _bucket.PageSize = 1;

        BackupCatalog catalog = NewCatalog();
        FakeObjectArtifactStore artifacts = Artifacts();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, ct: TestContext.Current.CancellationToken);

        IReadOnlyList<BackupArtifactEntry> listed =
            await artifacts.ListAsync(full.BackupId, TestContext.Current.CancellationToken);

        Assert.Equal(full.Checksums.Count, listed.Count);

        // And verification — which compares the declared set against the listing — still passes, which it
        // could not if pagination had truncated either side.
        await BackupArtifactVerifier.VerifyAsync(full, artifacts, TestContext.Current.CancellationToken);
    }
}

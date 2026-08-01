
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="BackupCatalog"/>, <see cref="BackupManifest"/>,
/// and <see cref="LocalDirectoryStorageTarget"/>.
/// Covers: Full→Inc→Inc chain resolution; contiguity validation; gap detection;
/// missing-manifest error; storage target round-trip.
/// </summary>
public sealed class TestPitrCatalog : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    private BackupCatalog NewCatalog(string name = "catalog") =>
        new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, name)));

    private static HLCTimestamp T(long ms) => new(0, ms, 0);

    private static PartitionBackupRange Range(int partitionId, long from, long to) =>
        PartitionBackupRange.Create(partitionId, from, T(from * 100), to, T(to * 100));

    // ── storage target round-trip ─────────────────────────────────────────────────

    [Fact]
    public void StorageTarget_PutAndGet_Roundtrip()
    {
        BackupCatalog cat = NewCatalog();
        BackupManifest m = BackupManifest.CreateFull([Range(1, 1, 10)]);
        cat.Put(m);

        BackupManifest? got = cat.Get(m.BackupId);
        Assert.NotNull(got);
        Assert.Equal(m.BackupId, got!.BackupId);
        Assert.Equal(BackupType.Full, got.Type);
        Assert.Single(got.PartitionRanges);
        Assert.Equal(1, got.PartitionRanges[0].FromIndex);
        Assert.Equal(10, got.PartitionRanges[0].ToIndex);
    }

    [Fact]
    public void StorageTarget_Get_MissingId_ReturnsNull()
    {
        BackupCatalog cat = NewCatalog();
        Assert.Null(cat.Get(Guid.NewGuid()));
    }

    [Fact]
    public void StorageTarget_List_ReturnsAllManifests()
    {
        BackupCatalog cat = NewCatalog();
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 5)]);
        BackupManifest inc = BackupManifest.CreateIncremental(full.BackupId, [Range(1, 6, 10)]);
        cat.Put(full);
        cat.Put(inc);

        IReadOnlyList<BackupManifest> all = cat.List(TestContext.Current.CancellationToken);
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public void Manifest_HlcTimestamps_RoundtripViaStorageTarget()
    {
        BackupCatalog cat = NewCatalog();
        HLCTimestamp fromHlc = new(1, 999_000, 7);
        HLCTimestamp toHlc = new(1, 1_000_000, 3);
        PartitionBackupRange r = PartitionBackupRange.Create(1, 1, fromHlc, 50, toHlc);
        BackupManifest m = BackupManifest.CreateFull([r]);
        cat.Put(m);

        BackupManifest? got = cat.Get(m.BackupId);
        PartitionBackupRange rr = got!.PartitionRanges[0];
        Assert.Equal(fromHlc, rr.FromHlc);
        Assert.Equal(toHlc, rr.ToHlc);
    }

    // ── chain resolution ──────────────────────────────────────────────────────────

    [Fact]
    public void ResolveChain_FullOnly_ReturnsSingleEntry()
    {
        BackupCatalog cat = NewCatalog();
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 10)]);
        cat.Put(full);

        IReadOnlyList<BackupManifest> chain = cat.ResolveChain(full.BackupId, TestContext.Current.CancellationToken);
        Assert.Single(chain);
        Assert.Equal(full.BackupId, chain[0].BackupId);
    }

    [Fact]
    public void ResolveChain_FullIncInc_ReturnsChronologicalOrder()
    {
        BackupCatalog cat = NewCatalog();
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 10)]);
        BackupManifest inc1 = BackupManifest.CreateIncremental(full.BackupId, [Range(1, 11, 20)]);
        BackupManifest inc2 = BackupManifest.CreateIncremental(inc1.BackupId, [Range(1, 21, 30)]);
        cat.Put(full);
        cat.Put(inc1);
        cat.Put(inc2);

        IReadOnlyList<BackupManifest> chain = cat.ResolveChain(inc2.BackupId, TestContext.Current.CancellationToken);

        Assert.Equal(3, chain.Count);
        Assert.Equal(full.BackupId, chain[0].BackupId);
        Assert.Equal(inc1.BackupId, chain[1].BackupId);
        Assert.Equal(inc2.BackupId, chain[2].BackupId);
    }

    [Fact]
    public void ResolveChain_MissingParent_Throws()
    {
        BackupCatalog cat = NewCatalog();
        // Store only the incremental — its parent is absent from the catalog.
        BackupManifest inc = BackupManifest.CreateIncremental(Guid.NewGuid(), [Range(1, 11, 20)]);
        cat.Put(inc);

        BackupChainException ex = Assert.Throws<BackupChainException>(() => cat.ResolveChain(inc.BackupId, TestContext.Current.CancellationToken));
        Assert.Contains("not found", ex.Message);
    }

    [Fact]
    public void ResolveChain_SelfParent_Throws()
    {
        BackupCatalog cat = NewCatalog();
        // A manifest that points to itself as its own parent.
        BackupManifest m = BackupManifest.CreateFull([Range(1, 1, 10)]);
        m.ParentBackupId = m.BackupId;
        cat.Put(m);

        BackupChainException ex = Assert.Throws<BackupChainException>(() => cat.ResolveChain(m.BackupId, TestContext.Current.CancellationToken));
        Assert.Contains("Cycle", ex.Message);
    }

    [Fact]
    public void ResolveChain_TwoNodeCycle_Throws()
    {
        BackupCatalog cat = NewCatalog();
        // A → B → A
        BackupManifest a = BackupManifest.CreateFull([Range(1, 1, 10)]);
        BackupManifest b = BackupManifest.CreateIncremental(a.BackupId, [Range(1, 11, 20)]);
        a.ParentBackupId = b.BackupId; // close the cycle
        cat.Put(a);
        cat.Put(b);

        BackupChainException ex = Assert.Throws<BackupChainException>(() => cat.ResolveChain(b.BackupId, TestContext.Current.CancellationToken));
        Assert.Contains("Cycle", ex.Message);
    }

    // ── chain validation ──────────────────────────────────────────────────────────

    [Fact]
    public void Validate_EmptyChain_Throws()
    {
        BackupChainException ex = Assert.Throws<BackupChainException>(() =>
            BackupCatalog.Validate([]));
        Assert.Contains("empty", ex.Message);
    }

    [Fact]
    public void Validate_ChainStartsWithIncremental_Throws()
    {
        BackupManifest inc = BackupManifest.CreateIncremental(Guid.NewGuid(), [Range(1, 1, 10)]);
        BackupChainException ex = Assert.Throws<BackupChainException>(() =>
            BackupCatalog.Validate([inc]));
        Assert.Contains("Full", ex.Message);
    }

    [Fact]
    public void Validate_ContiguousChain_Passes()
    {
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 10)]);
        BackupManifest inc1 = BackupManifest.CreateIncremental(full.BackupId, [Range(1, 11, 20)]);
        BackupManifest inc2 = BackupManifest.CreateIncremental(inc1.BackupId, [Range(1, 21, 30)]);

        // Should not throw.
        BackupCatalog.Validate([full, inc1, inc2]);
    }

    [Fact]
    public void Validate_IndexGap_Throws()
    {
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 10)]);
        // Gap: inc1 covers 12–20 but full only goes to 10 → FromIndex should be 11.
        BackupManifest inc1 = BackupManifest.CreateIncremental(full.BackupId, [Range(1, 12, 20)]);

        BackupChainException ex = Assert.Throws<BackupChainException>(() =>
            BackupCatalog.Validate([full, inc1]));
        Assert.Contains("gap", ex.Message.ToLowerInvariant());
        Assert.Contains("partition 1", ex.Message);
    }

    [Fact]
    public void Validate_BrokenParentLink_Throws()
    {
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 10)]);
        // inc1 declares a different parent than full.BackupId.
        BackupManifest inc1 = BackupManifest.CreateIncremental(Guid.NewGuid(), [Range(1, 11, 20)]);

        BackupChainException ex = Assert.Throws<BackupChainException>(() =>
            BackupCatalog.Validate([full, inc1]));
        Assert.Contains("parent", ex.Message.ToLowerInvariant());
    }

    [Fact]
    public void Validate_SecondEntryIsFullNotIncremental_Throws()
    {
        BackupManifest full1 = BackupManifest.CreateFull([Range(1, 1, 10)]);
        BackupManifest full2 = BackupManifest.CreateFull([Range(1, 11, 20)]);

        BackupChainException ex = Assert.Throws<BackupChainException>(() =>
            BackupCatalog.Validate([full1, full2]));
        Assert.Contains("Incremental", ex.Message);
    }

    [Fact]
    public void ResolveAndValidate_ContiguousChain_Passes()
    {
        BackupCatalog cat = NewCatalog();
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 10)]);
        BackupManifest inc1 = BackupManifest.CreateIncremental(full.BackupId, [Range(1, 11, 20)]);
        BackupManifest inc2 = BackupManifest.CreateIncremental(inc1.BackupId, [Range(1, 21, 30)]);
        cat.Put(full);
        cat.Put(inc1);
        cat.Put(inc2);

        IReadOnlyList<BackupManifest> chain = cat.ResolveAndValidate(inc2.BackupId, TestContext.Current.CancellationToken);

        Assert.Equal(3, chain.Count);
        Assert.Equal(BackupType.Full, chain[0].Type);
    }

    [Fact]
    public void Validate_MultiplePartitions_AllMustBeContiguous()
    {
        // Two partitions: P1 is fine, P2 has a gap.
        BackupManifest full = BackupManifest.CreateFull([Range(1, 1, 10), Range(2, 1, 5)]);
        BackupManifest inc = BackupManifest.CreateIncremental(full.BackupId,
        [
            Range(1, 11, 20), // contiguous
            Range(2, 8, 15)   // gap: should be 6
        ]);

        BackupChainException ex = Assert.Throws<BackupChainException>(() =>
            BackupCatalog.Validate([full, inc]));
        Assert.Contains("partition 2", ex.Message);
    }

    // ── owner / topology identity ──────────────────────────────────────────────────

    private static BackupManifest FullWithRange(long from, long to) => BackupManifest.CreateFull([Range(1, from, to)]);

    private static BackupManifest IncWithRange(Guid parent, long from, long to) =>
        BackupManifest.CreateIncremental(parent, [Range(1, from, to)]);

    [Fact]
    public void Manifest_Identity_RoundtripViaStorageTarget()
    {
        BackupCatalog cat = NewCatalog();
        BackupManifest m = FullWithRange(1, 10);
        const string cluster = "cluster-a";
        m.ApplyOwnerIdentity(new BackupOwnerIdentity(cluster, "node-a:8001", 7, "sqlite", "rev-3", 42));
        cat.Put(m);

        BackupManifest? got = cat.Get(m.BackupId);
        Assert.NotNull(got);
        Assert.Equal(cluster, got!.ClusterId);
        Assert.Equal("node-a:8001", got.CoordinatorNode);
        Assert.Equal(7, got.CoordinatorTerm);
        Assert.Equal("sqlite", got.StorageType);
        Assert.Equal("rev-3", got.StorageRevision);
        Assert.Equal(42, got.TopologyGeneration);
    }

    [Fact]
    public void ApplyOwnerIdentity_LeavesUnknownFieldsUntouched()
    {
        BackupManifest m = FullWithRange(1, 10);
        m.ApplyOwnerIdentity(new BackupOwnerIdentity(null, "node-a", null, "sqlite", "rev-1", null));

        // Only the supplied (non-null) fields are stamped; unknown ones stay null rather than a placeholder.
        Assert.Null(m.ClusterId);
        Assert.Null(m.CoordinatorTerm);
        Assert.Null(m.TopologyGeneration);
        Assert.Equal("node-a", m.CoordinatorNode);
        Assert.Equal("rev-1", m.StorageRevision);
    }

    [Fact]
    public void Validate_ConsistentIdentityChain_Passes()
    {
        const string cluster = "cluster-a";
        var id = new BackupOwnerIdentity(cluster, "node-a", 1, "sqlite", "rev-1", 5);

        BackupManifest full = FullWithRange(1, 10);
        BackupManifest inc = IncWithRange(full.BackupId, 11, 20);
        full.ApplyOwnerIdentity(id);
        inc.ApplyOwnerIdentity(id);

        // Same cluster / revision / topology across the chain: no throw.
        BackupCatalog.Validate([full, inc]);
    }

    [Fact]
    public void Validate_LegacyChainWithNoIdentity_Passes()
    {
        // Neither manifest carries any identity (legacy / pre-stamping) — identity guards must skip them.
        BackupManifest full = FullWithRange(1, 10);
        BackupManifest inc = IncWithRange(full.BackupId, 11, 20);
        BackupCatalog.Validate([full, inc]);
    }

    [Fact]
    public void Validate_MixedNullIdentity_SkipsUnknownAndPasses()
    {
        // The full carries a cluster id but the incremental's is unknown (null): unknown is skipped, not
        // forced to match, so the chain is accepted.
        BackupManifest full = FullWithRange(1, 10);
        BackupManifest inc = IncWithRange(full.BackupId, 11, 20);
        full.ApplyOwnerIdentity(new BackupOwnerIdentity("cluster-a", null, null, null, "rev-1", null));
        BackupCatalog.Validate([full, inc]);
    }

    [Fact]
    public void Validate_MismatchedClusterId_Throws()
    {
        BackupManifest full = FullWithRange(1, 10);
        BackupManifest inc = IncWithRange(full.BackupId, 11, 20);
        full.ApplyOwnerIdentity(new BackupOwnerIdentity("cluster-a", null, null, null, null, null));
        inc.ApplyOwnerIdentity(new BackupOwnerIdentity("cluster-b", null, null, null, null, null));

        BackupChainException ex = Assert.Throws<BackupChainException>(() => BackupCatalog.Validate([full, inc]));
        Assert.Contains("cluster id", ex.Message);
    }

    [Fact]
    public void Validate_MismatchedStorageRevision_Throws()
    {
        BackupManifest full = FullWithRange(1, 10);
        BackupManifest inc = IncWithRange(full.BackupId, 11, 20);
        full.ApplyOwnerIdentity(new BackupOwnerIdentity(null, null, null, "sqlite", "rev-1", null));
        inc.ApplyOwnerIdentity(new BackupOwnerIdentity(null, null, null, "sqlite", "rev-2", null));

        BackupChainException ex = Assert.Throws<BackupChainException>(() => BackupCatalog.Validate([full, inc]));
        Assert.Contains("storage revision", ex.Message);
    }

    [Fact]
    public void Validate_MismatchedTopologyGeneration_Throws()
    {
        BackupManifest full = FullWithRange(1, 10);
        BackupManifest inc = IncWithRange(full.BackupId, 11, 20);
        full.ApplyOwnerIdentity(new BackupOwnerIdentity(null, null, null, null, null, 5));
        inc.ApplyOwnerIdentity(new BackupOwnerIdentity(null, null, null, null, null, 6));

        BackupChainException ex = Assert.Throws<BackupChainException>(() => BackupCatalog.Validate([full, inc]));
        Assert.Contains("topology generation", ex.Message);
    }

    [Fact]
    public void Validate_DifferingCoordinatorNodeOrTerm_IsNotChainGating()
    {
        // Successive backups may be produced by different coordinators after a leadership change; the
        // node and term are informational and must NOT reject the chain.
        BackupManifest full = FullWithRange(1, 10);
        BackupManifest inc = IncWithRange(full.BackupId, 11, 20);
        full.ApplyOwnerIdentity(new BackupOwnerIdentity(null, "node-a", 1, null, "rev-1", null));
        inc.ApplyOwnerIdentity(new BackupOwnerIdentity(null, "node-b", 2, null, "rev-1", null));

        BackupCatalog.Validate([full, inc]); // no throw
    }
}

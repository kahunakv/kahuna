
using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Server.Tests;

/// <summary>
/// Chain-aware retention and orphan-artifact reclamation. Planning is pure over the catalog's
/// manifests, so these assert the plan (what would be deleted and why) directly, plus the on-disk
/// delete primitives and their crash-safety ordering.
/// </summary>
public sealed class TestBackupRetention : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_retention_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private static readonly DateTime Base = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static BackupManifest Full(Guid id, DateTime created, long bytes = 100)
    {
        BackupManifest m = BackupManifest.CreateFull([]);
        m.BackupId = id;
        m.CreatedAtUtc = created;
        m.Sizes["checkpoint/data"] = bytes;
        return m;
    }

    private static BackupManifest Inc(Guid id, Guid parent, DateTime created, long bytes = 10)
    {
        BackupManifest m = BackupManifest.CreateIncremental(parent, []);
        m.BackupId = id;
        m.CreatedAtUtc = created;
        m.Sizes["partition_1.wal"] = bytes;
        return m;
    }

    private static Guid G(int n) => new($"{n:D32}");

    private static HashSet<Guid> DeletedIds(IReadOnlyList<BackupGcCandidate> plan) =>
        plan.Select(c => c.BackupId).ToHashSet();

    // ── retention policy ─────────────────────────────────────────────────────────────────────

    [Fact]
    public void Disabled_DeletesNothing()
    {
        List<BackupManifest> ms = [Full(G(1), Base), Full(G(2), Base.AddHours(-100))];
        Assert.Empty(BackupRetention.PlanRetention(ms, BackupRetentionPolicy.Disabled, Base.AddHours(1)));
    }

    [Fact]
    public void MaxChains_KeepsNewestChainsWholeAndDeletesOlder()
    {
        // Three chains; chain A is Full(1)->Inc(2). Keep the 2 newest chains.
        Guid f1 = G(1), i2 = G(2), f3 = G(3), f4 = G(4);
        List<BackupManifest> ms =
        [
            Full(f1, Base.AddHours(-30)), Inc(i2, f1, Base.AddHours(-29)),  // chain A (newest tip -29)
            Full(f3, Base.AddHours(-20)),                                    // chain B (tip -20) newest
            Full(f4, Base.AddHours(-40)),                                    // chain C (tip -40) oldest
        ];

        IReadOnlyList<BackupGcCandidate> plan =
            BackupRetention.PlanRetention(ms, new BackupRetentionPolicy(MaxChains: 2, null, null), Base);

        // Chains B (-20) and A (-29) are the two newest → kept whole (f1 AND i2 survive). Chain C deleted.
        Assert.Equal([f4], DeletedIds(plan).ToList());
    }

    [Fact]
    public void MaxAge_DeletesChainsWhoseNewestBackupIsTooOld()
    {
        Guid f1 = G(1), i2 = G(2), f3 = G(3);
        List<BackupManifest> ms =
        [
            Full(f1, Base.AddHours(-50)), Inc(i2, f1, Base.AddMinutes(-30)), // tip is recent → kept whole
            Full(f3, Base.AddHours(-3)),                                     // tip 3h old → too old
        ];

        IReadOnlyList<BackupGcCandidate> plan = BackupRetention.PlanRetention(
            ms, new BackupRetentionPolicy(null, MaxAge: TimeSpan.FromHours(1), null), Base);

        // Chain A's leaf is 30 min old → the whole chain (incl. its 50h-old Full root) is retained.
        Assert.Equal([f3], DeletedIds(plan).ToList());
    }

    [Fact]
    public void MaxTotalBytes_KeepsNewestWithinBudget_ButAlwaysKeepsNewestChain()
    {
        // Each chain ~100 bytes. Budget 250 keeps the 2 newest; a budget below one chain still keeps the newest.
        Guid f1 = G(1), f2 = G(2), f3 = G(3);
        List<BackupManifest> ms =
        [
            Full(f1, Base.AddHours(-10), bytes: 100),
            Full(f2, Base.AddHours(-20), bytes: 100),
            Full(f3, Base.AddHours(-30), bytes: 100),
        ];

        IReadOnlyList<BackupGcCandidate> plan250 =
            BackupRetention.PlanRetention(ms, new BackupRetentionPolicy(null, null, MaxTotalBytes: 250), Base);
        Assert.Equal([f3], DeletedIds(plan250).ToList()); // keep f1(-10)+f2(-20)=200 ≤ 250, drop f3

        IReadOnlyList<BackupGcCandidate> plan10 =
            BackupRetention.PlanRetention(ms, new BackupRetentionPolicy(null, null, MaxTotalBytes: 10), Base);
        Assert.Equal([f2, f3], DeletedIds(plan10).OrderBy(x => x).ToList()); // newest (f1) kept despite > budget
    }

    [Fact]
    public void NeverDeletesFullStillReferencedByARetainedLeaf()
    {
        // Forked chain: Full(1) has two incremental branches, Inc(2) [old] and Inc(3) [new]. Keep 1 chain.
        Guid f1 = G(1), i2 = G(2), i3 = G(3);
        List<BackupManifest> ms =
        [
            Full(f1, Base.AddHours(-10)),
            Inc(i2, f1, Base.AddHours(-9)),   // older leaf
            Inc(i3, f1, Base.AddHours(-1)),   // newer leaf
        ];

        IReadOnlyList<BackupGcCandidate> plan =
            BackupRetention.PlanRetention(ms, new BackupRetentionPolicy(MaxChains: 1, null, null), Base);

        HashSet<Guid> deleted = DeletedIds(plan);
        // The newer leaf i3 is kept, pinning its root f1. Only the older branch i2 is deletable; f1 must survive.
        Assert.DoesNotContain(f1, deleted);
        Assert.Contains(i2, deleted);
        Assert.DoesNotContain(i3, deleted);
    }

    [Fact]
    public void DeleteOrder_IsDescendantsBeforeAncestors()
    {
        Guid f1 = G(1), i2 = G(2), i3 = G(3);
        List<BackupManifest> ms =
        [
            Full(f1, Base.AddHours(-30)), Inc(i2, f1, Base.AddHours(-29)), Inc(i3, i2, Base.AddHours(-28)),
        ];

        // MaxChains 0 deletes everything; order must be i3 (leaf) → i2 → f1 (root).
        IReadOnlyList<BackupGcCandidate> plan =
            BackupRetention.PlanRetention(ms, new BackupRetentionPolicy(MaxChains: 0, null, null), Base);

        Assert.Equal([i3, i2, f1], plan.Select(c => c.BackupId).ToList());
    }

    // ── orphan sweep ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void OrphanSweep_FlagsUnbackedDirsAndLeftovers_KeepsValidAndReserved()
    {
        Directory.CreateDirectory(_tempRoot);
        Guid valid = G(1), orphan = G(2), reserved = G(3);
        Directory.CreateDirectory(Path.Combine(_tempRoot, valid.ToString("N")));
        Directory.CreateDirectory(Path.Combine(_tempRoot, orphan.ToString("N")));
        Directory.CreateDirectory(Path.Combine(_tempRoot, reserved.ToString("N")));
        Directory.CreateDirectory(Path.Combine(_tempRoot, orphan.ToString("N") + ".staging_abcd1234"));
        Directory.CreateDirectory(Path.Combine(_tempRoot, "not-a-backup-dir"));
        File.WriteAllText(Path.Combine(_tempRoot, valid.ToString("N") + ".manifest.tmp_deadbeef"), "x");

        IReadOnlyList<OrphanSweepCandidate> plan = BackupRetention.PlanOrphanSweep(_tempRoot, validManifestIds: new HashSet<Guid> { valid }, reservedIds: new HashSet<Guid> { reserved }, ct: TestContext.Current.CancellationToken);

        HashSet<string> names = plan.Select(c => Path.GetFileName(c.Path)).ToHashSet();
        Assert.Contains(orphan.ToString("N"), names);                      // no manifest → orphan
        Assert.Contains(orphan.ToString("N") + ".staging_abcd1234", names); // staging leftover
        Assert.Contains(valid.ToString("N") + ".manifest.tmp_deadbeef", names); // temp manifest file
        Assert.DoesNotContain(valid.ToString("N"), names);                 // has a manifest → kept
        Assert.DoesNotContain(reserved.ToString("N"), names);              // in-flight → kept
        Assert.DoesNotContain("not-a-backup-dir", names);                  // not ours → untouched
    }

    [Fact]
    public void OrphanSweep_CorruptManifest_ArtifactDirectoryIsProtected()
    {
        Directory.CreateDirectory(_tempRoot);
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(_tempRoot));

        Guid id = G(7);
        catalog.Put(Full(id, Base));                                              // writes {id}.manifest
        Directory.CreateDirectory(Path.Combine(_tempRoot, id.ToString("N")));     // its artifact directory

        // Corrupt the manifest file so the parsed listing can no longer read it.
        File.WriteAllText(Path.Combine(_tempRoot, id.ToString("N") + ".manifest"), "{ broken ");

        // The parsed listing loses the id, but the filename-only scan still owns it.
        Assert.DoesNotContain(id, catalog.List(TestContext.Current.CancellationToken).Select(m => m.BackupId));
        Assert.Contains(id, catalog.ListManifestIds(TestContext.Current.CancellationToken));

        HashSet<Guid> valid = catalog.List(TestContext.Current.CancellationToken).Select(m => m.BackupId).ToHashSet();
        HashSet<Guid> protectedIds = catalog.ListManifestIds(TestContext.Current.CancellationToken).ToHashSet();

        // Protected by manifest presence → the directory is NOT swept.
        IReadOnlyList<OrphanSweepCandidate> plan =
            BackupRetention.PlanOrphanSweep(_tempRoot, valid, protectedIds, TestContext.Current.CancellationToken);
        Assert.DoesNotContain(id.ToString("N"), plan.Select(c => Path.GetFileName(c.Path)));

        // Without that protection (the pre-fix behavior) the same directory WOULD have been destroyed.
        IReadOnlyList<OrphanSweepCandidate> unprotected =
            BackupRetention.PlanOrphanSweep(_tempRoot, valid, new HashSet<Guid>(), TestContext.Current.CancellationToken);
        Assert.Contains(id.ToString("N"), unprotected.Select(c => Path.GetFileName(c.Path)));
    }

    // ── delete primitives (on disk) ──────────────────────────────────────────────────────────

    [Fact]
    public void CatalogDelete_RemovesManifestAndArtifacts_Idempotently()
    {
        Directory.CreateDirectory(_tempRoot);
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(_tempRoot));
        Guid id = G(1);
        catalog.Put(Full(id, Base));
        string artifactDir = Path.Combine(_tempRoot, id.ToString("N"));
        Directory.CreateDirectory(artifactDir);
        File.WriteAllText(Path.Combine(artifactDir, "checkpoint"), "data");

        catalog.Delete(id, _tempRoot);

        Assert.Null(catalog.Get(id));
        Assert.False(Directory.Exists(artifactDir));

        // Idempotent: a second delete of an absent backup is a no-op, not an error.
        catalog.Delete(id, _tempRoot);
    }

    [Fact]
    public void CatalogDelete_ManifestFirst_CrashAfterTombstoneLeavesReclaimableOrphan()
    {
        Directory.CreateDirectory(_tempRoot);
        LocalDirectoryStorageTarget target = new(_tempRoot);
        BackupCatalog catalog = new(target);
        Guid id = G(1);
        catalog.Put(Full(id, Base));
        string artifactDir = Path.Combine(_tempRoot, id.ToString("N"));
        Directory.CreateDirectory(artifactDir);

        // Simulate a crash after the manifest tombstone but before the artifact dir is removed.
        target.Delete(id);

        // No manifest resolves to the (still-present) artifacts — the invariant holds — and the orphan
        // sweep now reclaims the directory.
        Assert.Null(catalog.Get(id));
        IReadOnlyList<OrphanSweepCandidate> sweep =
            BackupRetention.PlanOrphanSweep(_tempRoot, new HashSet<Guid>(), new HashSet<Guid>(), TestContext.Current.CancellationToken);
        Assert.Contains(id.ToString("N"), sweep.Select(c => Path.GetFileName(c.Path)));

        BackupRetention.ApplyOrphanSweep(sweep, TestContext.Current.CancellationToken);
        Assert.False(Directory.Exists(artifactDir));
    }
}

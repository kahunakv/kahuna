
using Kommander;
using Kommander.Time;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that PruneKeyValueRevisions respects the snapshot floor on every persistent backend:
/// the floor-boundary revision and every revision newer than it are never deleted regardless of the
/// count/age retention policy. Runs against both SQLite and RocksDB — the two backends compute the
/// floor boundary by different mechanisms (SQL comparison vs. deserialized-HLC LINQ) and must agree.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestSnapshotFloorPersistentPrune
{
    private static (IPersistenceBackend Backend, string Path) CreateBackend(string storage)
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_floorprune_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);

        IPersistenceBackend backend = storage switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(dir, "fp"),
            "sqlite"  => new SqlitePersistenceBackend(dir, "fp"),
            _         => throw new ArgumentException($"unknown storage '{storage}'", nameof(storage))
        };

        return (backend, dir);
    }

    /// <summary>
    /// Builds a PersistenceRequestItem with an explicit HLC LastModified. The revision equals the
    /// physical component / 1000 so that T1 &lt; T2 &lt; … in both HLC order and revision order.
    /// </summary>
    private static PersistenceRequestItem MakeItemAtHlc(string key, long revision, HLCTimestamp hlc) =>
        new(key,
            System.Text.Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: hlc.N,
            lastModifiedPhysical: hlc.L,
            lastModifiedCounter: (uint)hlc.C,
            state: (int)KeyValueState.Set);

    private static bool RevisionExists(IPersistenceBackend backend, string key, long revision) =>
        backend.GetKeyValueRevision(key, revision) is not null;

    private static void StoreFiveRevisions(IPersistenceBackend backend, string key)
    {
        for (long i = 1; i <= 5; i++)
            backend.StoreKeyValues([MakeItemAtHlc(key, i, new(1, i * 1000L, 0))]);
    }

    // ── tests ────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Floor set to T2 (physical=2000). Revisions 1–5 stored (T1..T5). Floor boundary = rev 2 (highest
    /// revision with LastModified ≤ T2). With retentionCount=2 the standard policy would keep only rev
    /// 4+5; with the floor, rev 2–5 must be kept and only rev 1 may be deleted.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void FloorProtectsBoundaryAndNewerRevisions(string storage)
    {
        const string key = "floor/prune/test";
        (IPersistenceBackend backend, _) = CreateBackend(storage);
        try
        {
            StoreFiveRevisions(backend, key);

            // Floor = T2 → boundary rev 2; retentionCount=2 would otherwise keep only rev 4,5.
            Assert.True(backend.PruneKeyValueRevisions(
                [key], retentionCount: 2, TimeSpan.Zero, batchSize: 1000, new HLCTimestamp(1, 2000L, 0),
                out RevisionPruneResult result));

            Assert.False(result.BatchLimitReached);
            Assert.Equal(0, result.FloorViolations); // rev 1 is below the boundary; deleting it is not a violation

            Assert.False(RevisionExists(backend, key, 1), "revision 1 (below boundary, outside retention) should be pruned");
            Assert.True(RevisionExists(backend, key, 2),  "revision 2 (floor boundary) must be kept");
            Assert.True(RevisionExists(backend, key, 3),  "revision 3 must be kept (newer than floor)");
            Assert.True(RevisionExists(backend, key, 4),  "revision 4 must be kept (newer than floor)");
            Assert.True(RevisionExists(backend, key, 5),  "revision 5 (current) must always be kept");
        }
        finally { (backend as IDisposable)?.Dispose(); }
    }

    /// <summary>
    /// When floorTimestamp == HLCTimestamp.Zero the existing policy applies with no floor protection:
    /// retentionCount=2 keeps only the two newest history rows (rev 4,5).
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void ZeroFloorUsesStandardRetentionWithNoProtection(string storage)
    {
        const string key = "floor/zero/test";
        (IPersistenceBackend backend, _) = CreateBackend(storage);
        try
        {
            StoreFiveRevisions(backend, key);

            Assert.True(backend.PruneKeyValueRevisions(
                [key], retentionCount: 2, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero,
                out RevisionPruneResult result));

            Assert.False(result.BatchLimitReached);
            Assert.Equal(0, result.FloorViolations); // no floor active → the audit never counts

            // No floor → the three oldest history revisions are pruned by the count policy.
            Assert.False(RevisionExists(backend, key, 1), "revision 1 should be pruned");
            Assert.False(RevisionExists(backend, key, 2), "revision 2 should be pruned");
            Assert.False(RevisionExists(backend, key, 3), "revision 3 should be pruned");
            Assert.True(RevisionExists(backend, key, 4),  "revision 4 must be kept");
            Assert.True(RevisionExists(backend, key, 5),  "revision 5 (current) must be kept");
        }
        finally { (backend as IDisposable)?.Dispose(); }
    }

    /// <summary>
    /// When the floor equals the oldest revision's LastModified, the boundary is rev 1 and the prunable
    /// window is empty (revision &lt; 1 matches nothing). No revisions are deleted even with retentionCount=1.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void FloorAtOldestRevisionBlocksAllDeletions(string storage)
    {
        const string key = "floor/all/test";
        (IPersistenceBackend backend, _) = CreateBackend(storage);
        try
        {
            StoreFiveRevisions(backend, key);

            // Floor at T1 → boundary is rev 1 (oldest); nothing is prunable.
            Assert.True(backend.PruneKeyValueRevisions(
                [key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, new HLCTimestamp(1, 1000L, 0),
                out RevisionPruneResult result));

            Assert.False(result.BatchLimitReached);
            Assert.Equal(0, result.RevisionsDeleted);
            Assert.Equal(0, result.FloorViolations); // nothing prunable → no protected revision deleted

            for (long i = 1; i <= 5; i++)
                Assert.True(RevisionExists(backend, key, i), $"revision {i} must still exist");
        }
        finally { (backend as IDisposable)?.Dispose(); }
    }

    /// <summary>
    /// Key created entirely after the floor (all revisions have LastModified &gt; floorTimestamp): the
    /// boundary query finds no revision at-or-before the floor. The floor is still active, so every
    /// revision is "after the floor" and must be protected — zero deletions, regardless of retention.
    /// This is the regression guard for the "no boundary ⟹ protection disabled" bug.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void FloorActiveButKeyCreatedAfter_AllRevisionsProtected(string storage)
    {
        const string key = "floor/after/test";
        (IPersistenceBackend backend, _) = CreateBackend(storage);
        try
        {
            StoreFiveRevisions(backend, key);

            // Floor at T=500 — before any revision (T1=1000). retentionCount=2 would delete rev 1–3
            // without floor protection; with the floor active, all five must be kept.
            Assert.True(backend.PruneKeyValueRevisions(
                [key], retentionCount: 2, TimeSpan.Zero, batchSize: 1000, new HLCTimestamp(1, 500L, 0),
                out RevisionPruneResult result));

            Assert.False(result.BatchLimitReached);
            Assert.Equal(0, result.RevisionsDeleted);
            Assert.Equal(0, result.FloorViolations); // floorRevision = -1 (all protected) → audit stays 0

            for (long i = 1; i <= 5; i++)
                Assert.True(RevisionExists(backend, key, i), $"revision {i} must be kept (after floor, all protected)");
        }
        finally { (backend as IDisposable)?.Dispose(); }
    }
}

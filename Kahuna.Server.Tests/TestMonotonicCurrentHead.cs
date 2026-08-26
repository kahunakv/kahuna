
using Kommander.Time;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Tests;

/// <summary>
/// The durable current row per key must advance monotonically by (revision, commit HLC). The same
/// committed mutation is queued by both the owning actor and the Raft consumer, so a delayed older
/// duplicate can reach a backend after a newer head — inside one batch or across batches — and it
/// must never regress what a read serves as current. An older record still lands as retained
/// history: snapshot reads and point-in-time recovery may need it. Retained-history rows keyed by
/// (key, revision) advance by commit HLC only, because delete and extend records legitimately
/// reuse a revision number. A regressed current marker once froze scan-visible rows behind their
/// committed heads while the settle-time overlay witness had already passed and removed the only
/// parked repair — so this invariant is what keeps a committed head scan-visible.
/// </summary>
public class TestMonotonicCurrentHead
{
    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private static PersistenceRequestItem Item(
        string key,
        long revision,
        long lastModifiedPhysical,
        KeyValueState state = KeyValueState.Set,
        string? value = "unset",
        bool noRevision = false) =>
        new(key,
            value is null ? null : System.Text.Encoding.UTF8.GetBytes(value == "unset" ? $"val{revision}@{lastModifiedPhysical}" : value),
            revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: lastModifiedPhysical, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: lastModifiedPhysical, lastModifiedCounter: 0,
            state: (int)state,
            noRevision: noRevision);

    private static string TempPath(string kind)
    {
        string dir = Path.Combine(Path.GetTempPath(), $"kahuna_mono_{kind}_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static IPersistenceBackend CreateBackend(string kind, string? path = null) => kind switch
    {
        "memory" => new MemoryPersistenceBackend(),
        "sqlite" => new SqlitePersistenceBackend(path ?? TempPath(kind), "v1"),
        "rocksdb" => new RocksDbPersistenceBackend(path ?? TempPath(kind), "v1"),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void OlderDuplicateInTheSameBatchNeverRegressesTheCurrentRow(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            const string key = "mono/same-batch";

            // The newer head arrives FIRST in the batch; the delayed older duplicate is last.
            // A last-write-wins store would leave the current row at 310.
            Assert.True(backend.StoreKeyValues([
                Item(key, revision: 311, lastModifiedPhysical: 2_000),
                Item(key, revision: 310, lastModifiedPhysical: 1_000)
            ]));

            KeyValueEntry? current = backend.GetKeyValue(key);
            Assert.NotNull(current);
            Assert.Equal(311, current!.Revision);
            Assert.Equal("val311@2000", System.Text.Encoding.UTF8.GetString(current.Value!));

            // Both records remain readable as retained history.
            Assert.Equal(310, backend.GetKeyValueRevision(key, 310)?.Revision);
            Assert.Equal(311, backend.GetKeyValueRevision(key, 311)?.Revision);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void OlderDuplicateInALaterBatchNeverRegressesTheCurrentRow(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            const string key = "mono/cross-batch";

            Assert.True(backend.StoreKeyValues([Item(key, revision: 310, lastModifiedPhysical: 1_000)]));
            Assert.True(backend.StoreKeyValues([Item(key, revision: 311, lastModifiedPhysical: 2_000)]));

            // The delayed duplicate of the older commit lands after the newer head already flushed.
            Assert.True(backend.StoreKeyValues([Item(key, revision: 310, lastModifiedPhysical: 1_000)]));

            KeyValueEntry? current = backend.GetKeyValue(key);
            Assert.NotNull(current);
            Assert.Equal(311, current!.Revision);

            Assert.Equal(310, backend.GetKeyValueRevision(key, 310)?.Revision);
            Assert.Equal(311, backend.GetKeyValueRevision(key, 311)?.Revision);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void SameRevisionRecordsAdvanceByCommitHlcOnly(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            const string key = "mono/same-revision";

            // A delete reuses the set's revision number with a newer commit HLC.
            Assert.True(backend.StoreKeyValues([Item(key, revision: 311, lastModifiedPhysical: 2_000)]));
            Assert.True(backend.StoreKeyValues([Item(key, revision: 311, lastModifiedPhysical: 3_000, state: KeyValueState.Deleted, value: null)]));

            KeyValueEntry? current = backend.GetKeyValue(key);
            Assert.NotNull(current);
            Assert.Equal(KeyValueState.Deleted, current!.State);
            Assert.Equal(Ts(3_000), current.LastModified);

            // The delayed duplicate of the superseded set must regress neither the current row nor
            // the (key, revision) history row the delete owns.
            Assert.True(backend.StoreKeyValues([Item(key, revision: 311, lastModifiedPhysical: 2_000)]));

            current = backend.GetKeyValue(key);
            Assert.NotNull(current);
            Assert.Equal(KeyValueState.Deleted, current!.State);
            Assert.Equal(Ts(3_000), current.LastModified);

            KeyValueEntry? history = backend.GetKeyValueRevision(key, 311);
            Assert.NotNull(history);
            Assert.Equal(KeyValueState.Deleted, history!.State);
            Assert.Equal(Ts(3_000), history.LastModified);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void SameBatchSameRevisionRecordsResolveToTheHlcWinner(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            // Newer-first and older-first batch orders must both resolve to the HLC winner.
            const string keyA = "mono/hlc-winner-a";
            Assert.True(backend.StoreKeyValues([
                Item(keyA, revision: 311, lastModifiedPhysical: 3_000, state: KeyValueState.Deleted, value: null),
                Item(keyA, revision: 311, lastModifiedPhysical: 2_000)
            ]));

            const string keyB = "mono/hlc-winner-b";
            Assert.True(backend.StoreKeyValues([
                Item(keyB, revision: 311, lastModifiedPhysical: 2_000),
                Item(keyB, revision: 311, lastModifiedPhysical: 3_000, state: KeyValueState.Deleted, value: null)
            ]));

            foreach (string key in (string[])[keyA, keyB])
            {
                KeyValueEntry? current = backend.GetKeyValue(key);
                Assert.NotNull(current);
                Assert.Equal(KeyValueState.Deleted, current!.State);
                Assert.Equal(Ts(3_000), current.LastModified);

                KeyValueEntry? history = backend.GetKeyValueRevision(key, 311);
                Assert.NotNull(history);
                Assert.Equal(KeyValueState.Deleted, history!.State);
                Assert.Equal(Ts(3_000), history.LastModified);
            }
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void NoRevisionWritesKeepTheNewestHeadAndRetainNoHistory(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            const string key = "mono/no-revision";

            Assert.True(backend.StoreKeyValues([Item(key, revision: 7, lastModifiedPhysical: 2_000, value: "new", noRevision: true)]));

            // The delayed older duplicate carries the same revision with an older commit HLC.
            Assert.True(backend.StoreKeyValues([Item(key, revision: 7, lastModifiedPhysical: 1_000, value: "old", noRevision: true)]));

            KeyValueEntry? current = backend.GetKeyValue(key);
            Assert.NotNull(current);
            Assert.Equal(Ts(2_000), current!.LastModified);
            Assert.Equal("new", System.Text.Encoding.UTF8.GetString(current.Value!));

            // No-revision writes retain no history row.
            Assert.Null(backend.GetKeyValueRevision(key, 7));
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void ReplayAfterReopenNeverRegressesTheCurrentRow(string kind)
    {
        // Restart replay re-delivers committed records the durability floor has not certified: an
        // older record can be re-stored after the durable head already contains a newer one.
        string path = TempPath(kind);
        const string key = "mono/reopen";

        IPersistenceBackend backend = CreateBackend(kind, path);
        try
        {
            Assert.True(backend.StoreKeyValues([Item(key, revision: 311, lastModifiedPhysical: 2_000)]));
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }

        IPersistenceBackend reopened = CreateBackend(kind, path);
        try
        {
            Assert.True(reopened.StoreKeyValues([Item(key, revision: 310, lastModifiedPhysical: 1_000)]));

            KeyValueEntry? current = reopened.GetKeyValue(key);
            Assert.NotNull(current);
            Assert.Equal(311, current!.Revision);

            Assert.Equal(310, reopened.GetKeyValueRevision(key, 310)?.Revision);
            Assert.Equal(311, reopened.GetKeyValueRevision(key, 311)?.Revision);
        }
        finally
        {
            (reopened as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// The run-X failure shape end to end at the storage layer: the overlay held the newest head,
    /// a flush batch carried the head plus a delayed older duplicate, and a successful store both
    /// pruned the overlay and (pre-fix) let the duplicate win the backend's current row — after
    /// which reads served the regressed row with nothing left to mask it. With the monotonic store
    /// the pruned overlay must never expose anything below the batch's newest head.
    /// </summary>
    [Fact]
    public void OverlayCleanupNeverExposesARegressedCurrentRow()
    {
        using MemoryPersistenceBackend inner = new();
        UnflushedKeyValueWritesIndex overlay = new();
        UnflushedOverlayPersistenceBackend decorated = new(inner, overlay, new UnflushedLockWritesIndex());

        const string key = "mono/overlay";

        overlay.Record(key, "old"u8.ToArray(), 310, HLCTimestamp.Zero, Ts(1_000), Ts(1_000), KeyValueState.Set, noRevision: false);
        overlay.Record(key, "new"u8.ToArray(), 311, HLCTimestamp.Zero, Ts(2_000), Ts(2_000), KeyValueState.Set, noRevision: false);

        // Queued but unflushed: reads already serve the newest head from the overlay.
        Assert.Equal(311, decorated.GetKeyValue(key)?.Revision);

        // The flush batch carries the head first and the delayed older duplicate last.
        Assert.True(decorated.StoreKeyValues([
            Item(key, revision: 311, lastModifiedPhysical: 2_000, value: "new"),
            Item(key, revision: 310, lastModifiedPhysical: 1_000, value: "old")
        ]));

        // The confirmed flush pruned the overlay — and the backend row it stops masking must be
        // the batch's newest head, not the delayed duplicate.
        Assert.False(overlay.TryGet(key, out _));
        Assert.Equal(311, inner.GetKeyValue(key)?.Revision);
        Assert.Equal(311, decorated.GetKeyValue(key)?.Revision);
        Assert.Equal("new", System.Text.Encoding.UTF8.GetString(decorated.GetKeyValue(key)!.Value!));
    }
}

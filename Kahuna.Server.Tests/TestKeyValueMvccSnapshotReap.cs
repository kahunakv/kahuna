
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that zero-Expires MVCC snapshots created by read-only transactions are reclaimed
/// when the owning session is known to be dead — committed, rolled-back, or expired by age —
/// regardless of whether the client supplied a read-key list at commit time.
///
/// The trim fires as a side-effect of the next commit / rollback / lock-release on the same key,
/// which mirrors real-world cadence: a new writer on the same key triggers cleanup of any stale
/// read snapshots left by a previous reader.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestKeyValueMvccSnapshotReap
{
    // ── committed-session path ───────────────────────────────────────────────────────────

    /// <summary>
    /// A zero-Expires MVCC snapshot whose transaction was recorded as committed on this actor
    /// is removed when the next release touches the same key.
    /// </summary>
    [Fact]
    public async Task ZeroExpiresSnapshot_RemovedWhenOwningTransactionCommitted()
    {
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Reader transaction that created a zero-Expires MVCC snapshot (non-expiring key read).
        HLCTimestamp readerTx = new(now.N, now.L - 1000, 0);  // 1 s ago (L is in ms)

        // A second transaction that holds the write intent and triggers the release.
        HLCTimestamp writerTx = now;

        InsertWithMvcc(context, "snap/key", writerTx, readerTx, HLCTimestamp.Zero /* zero-Expires */);

        // Mark the reader as committed on this actor.
        context.RecordCommitted(readerTx);

        // Release the writer's lock → TrimExpiredMvccEntries fires.
        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "snap/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        KeyValueEntry? entry = context.Store.Get("snap/key");
        Assert.NotNull(entry);
        Assert.True(
            entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(readerTx),
            "zero-Expires snapshot for a committed transaction must be removed by the trim");
    }

    // ── rolled-back-session path ─────────────────────────────────────────────────────────

    /// <summary>
    /// A zero-Expires MVCC snapshot whose transaction was recorded as rolled back on this actor
    /// is removed when the next release touches the same key.
    /// </summary>
    [Fact]
    public async Task ZeroExpiresSnapshot_RemovedWhenOwningTransactionRolledBack()
    {
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp readerTx = new(now.N, now.L - 1000, 0);  // 1 s ago
        HLCTimestamp writerTx = now;

        InsertWithMvcc(context, "snap/key", writerTx, readerTx, HLCTimestamp.Zero);

        // Mark the reader as rolled back on this actor.
        context.RecordRolledBack(readerTx);

        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "snap/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        KeyValueEntry? entry = context.Store.Get("snap/key");
        Assert.NotNull(entry);
        Assert.True(
            entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(readerTx),
            "zero-Expires snapshot for a rolled-back transaction must be removed by the trim");
    }

    // ── abandoned-session path (age-based reap) ──────────────────────────────────────────

    /// <summary>
    /// A zero-Expires MVCC snapshot whose transaction is older than the maximum possible session
    /// lifetime (default timeout + reap grace + effect TTL) is reclaimed even when neither commit
    /// nor rollback was recorded locally. This covers the legacy interactive path where the client
    /// never forwards a read-key list.
    /// </summary>
    [Fact]
    public async Task ZeroExpiresSnapshot_RemovedWhenOwningTransactionOlderThanMaxLifespan()
    {
        KahunaConfiguration config = CreateConfiguration(defaultTransactionTimeoutMs: 5000);
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Max session lifespan = 5000 + ReapGraceMs(15000) + MaxParticipantEffectTtlMs(15000) = 35000ms.
        // Place readerTx 36000ms in the past so it's definitively beyond the window.
        long pastMs = TransactionCoordinator.ReapGraceMs + TransactionCoordinator.MaxParticipantEffectTtlMs + 5000 + 1000;
        HLCTimestamp readerTx = new(now.N, now.L - pastMs, 0);  // L is in ms

        HLCTimestamp writerTx = now;

        InsertWithMvcc(context, "snap/key", writerTx, readerTx, HLCTimestamp.Zero);

        // Do NOT record commit or rollback — simulate an abandoned session.

        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "snap/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        KeyValueEntry? entry = context.Store.Get("snap/key");
        Assert.NotNull(entry);
        Assert.True(
            entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(readerTx),
            "zero-Expires snapshot for an abandoned transaction past the max session lifespan must be reaped");
    }

    // ── live-session preservation ────────────────────────────────────────────────────────

    /// <summary>
    /// A zero-Expires MVCC snapshot whose transaction is young (well within the max session
    /// lifespan) and has neither committed nor rolled back must be preserved.
    /// </summary>
    [Fact]
    public async Task ZeroExpiresSnapshot_PreservedForLiveTransaction()
    {
        KahunaConfiguration config = CreateConfiguration(defaultTransactionTimeoutMs: 5000);
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Place readerTx only 1 second in the past — clearly within the 35-second max lifespan.
        HLCTimestamp readerTx = new(now.N, now.L - 1000, 0);  // 1 s ago (L is in ms)
        HLCTimestamp writerTx = now;

        InsertWithMvcc(context, "snap/key", writerTx, readerTx, HLCTimestamp.Zero);

        // No commit, no rollback recorded — session still live by age.

        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "snap/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        KeyValueEntry? entry = context.Store.Get("snap/key");
        Assert.NotNull(entry);
        // The reader's snapshot must still be present.
        Assert.True(
            entry.MvccEntries is not null && entry.MvccEntries.ContainsKey(readerTx),
            "zero-Expires snapshot for a live (young) transaction must not be reaped prematurely");
    }

    // ── expiring-key snapshots are unaffected ────────────────────────────────────────────

    /// <summary>
    /// A non-zero Expires MVCC snapshot (for an expiring key) that has not yet elapsed is
    /// preserved regardless of transaction age. The zero-Expires path must not touch it.
    /// </summary>
    [Fact]
    public async Task NonZeroExpiresSnapshot_PreservedWhenNotElapsed()
    {
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // A very old transaction ID — would be reaped by the age-based check if Expires were zero.
        HLCTimestamp readerTx = new(now.N, now.L - 300_000, 0);  // 5 min ago (L is in ms)

        // Expires is in the future → must not be trimmed.
        HLCTimestamp futureExpires = new(now.N, now.L + 60_000, 0);  // 60 s from now

        HLCTimestamp writerTx = now;

        InsertWithMvcc(context, "snap/key", writerTx, readerTx, futureExpires);

        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "snap/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        KeyValueEntry? entry = context.Store.Get("snap/key");
        Assert.NotNull(entry);
        Assert.True(
            entry.MvccEntries is not null && entry.MvccEntries.ContainsKey(readerTx),
            "a non-zero Expires snapshot that has not elapsed must not be reaped");
    }

    // ── multiple snapshots: only dead sessions reclaimed ─────────────────────────────────

    /// <summary>
    /// When a key has MVCC snapshots for multiple transactions — some live, some dead — only the
    /// dead-session snapshots are removed. Live snapshots survive unharmed.
    /// </summary>
    [Fact]
    public async Task MultipleMvccEntries_OnlyDeadSessionSnapshotsRemoved()
    {
        KahunaConfiguration config = CreateConfiguration(defaultTransactionTimeoutMs: 5000);
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Dead by committed record.
        HLCTimestamp committedTx = new(now.N, now.L - 2000, 0);  // 2 s ago
        context.RecordCommitted(committedTx);

        // Dead by age (36 s ago — beyond 35 s max lifespan).
        long pastMs = TransactionCoordinator.ReapGraceMs + TransactionCoordinator.MaxParticipantEffectTtlMs + 5000 + 1000;
        HLCTimestamp abandonedTx = new(now.N, now.L - pastMs, 0);

        // Still live (500 ms ago — well within 35 s max lifespan).
        HLCTimestamp liveTx = new(now.N, now.L - 500, 0);

        HLCTimestamp writerTx = now;

        // Insert all three snapshots onto the same key.
        KeyValueEntry seedEntry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            LastModified = now,
            Revision = 1,
            FlushedRevision = 1,
            WriteIntent = new KeyValueWriteIntent { TransactionId = writerTx, Expires = new HLCTimestamp(now.N, now.L + 60_000, 0) }
        };
        context.InsertStoreEntry("snap/key", seedEntry);

        void AddSnapshot(HLCTimestamp txId)
        {
            seedEntry.MvccEntries ??= new();
            bool dictNew = seedEntry.MvccEntries.Count == 0;
            seedEntry.MvccEntries[txId] = new KeyValueMvccEntry
            {
                State = KeyValueState.Set, Revision = 0,
                Value = Encoding.UTF8.GetBytes("snap"),
                Expires = HLCTimestamp.Zero
            };
            context.AdjustEstimatedEntryBytes(seedEntry,
                KeyValueStoreAccounting.MvccEntryAddedBytes(dictNew, seedEntry.MvccEntries[txId].Value));
        }

        AddSnapshot(committedTx);
        AddSnapshot(abandonedTx);
        AddSnapshot(liveTx);

        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "snap/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        KeyValueEntry? after = context.Store.Get("snap/key");
        Assert.NotNull(after);

        Assert.True(after.MvccEntries is null || !after.MvccEntries.ContainsKey(committedTx),
            "committed session snapshot must be removed");
        Assert.True(after.MvccEntries is null || !after.MvccEntries.ContainsKey(abandonedTx),
            "age-expired abandoned session snapshot must be removed");
        Assert.True(after.MvccEntries is not null && after.MvccEntries.ContainsKey(liveTx),
            "live session snapshot must be preserved");
    }

    // ── write-intent cleanup is unchanged ────────────────────────────────────────────────

    /// <summary>
    /// A key that is actually modified (write intent present) still follows the existing write-intent
    /// cleanup path. The zero-Expires trim must not interfere with or skip a committed write entry.
    /// </summary>
    [Fact]
    public async Task WriteIntentEntry_UnaffectedByZeroExpiresTrim()
    {
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp writerTx = now;

        // Entry with a write intent and a staged mutation MVCC entry (Expires = non-zero intent lease).
        HLCTimestamp intentExpires = new(now.N, now.L + 15_000, 0);  // 15 s from now (L is in ms)
        KeyValueEntry seedEntry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            LastModified = now,
            Revision = 1,
            FlushedRevision = 1,
            WriteIntent = new KeyValueWriteIntent { TransactionId = writerTx, Expires = intentExpires }
        };
        context.InsertStoreEntry("mod/key", seedEntry);

        seedEntry.MvccEntries ??= new();
        bool dictNew = seedEntry.MvccEntries.Count == 0;
        seedEntry.MvccEntries[writerTx] = new KeyValueMvccEntry
        {
            State = KeyValueState.Set, Revision = 1,
            Value = Encoding.UTF8.GetBytes("new-val"),
            Expires = intentExpires  // non-zero: standard write-intent lease
        };
        context.AdjustEstimatedEntryBytes(seedEntry,
            KeyValueStoreAccounting.MvccEntryAddedBytes(dictNew, seedEntry.MvccEntries[writerTx].Value));

        // Release the writer's lock; it removes writerTx's own MVCC entry, then trims.
        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "mod/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        // The write-intent MVCC entry was removed by the handler's own Remove() call, not by our trim.
        // Trim must not have doubled-removed or corrupted anything.
        KeyValueEntry? after = context.Store.Get("mod/key");
        Assert.NotNull(after);
        // writerTx entry removed (by handler's explicit Remove), no extra junk added.
        Assert.True(after.MvccEntries is null || !after.MvccEntries.ContainsKey(writerTx),
            "writer's own MVCC entry must be removed by the release handler");
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static (TryReleaseExclusiveLockHandler, KeyValueContext, RaftManager) CreateHandler(
        KahunaConfiguration? config = null)
    {
        config ??= CreateConfiguration();

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "mvcc-reap-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        KeyValueContext context = new(
            null!,
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            null!,
            raft,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger
        );

        return (new TryReleaseExclusiveLockHandler(context), context, raft);
    }

    private static KahunaConfiguration CreateConfiguration(int defaultTransactionTimeoutMs = 5000)
    {
        KahunaConfiguration cfg = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
        });
        cfg.DefaultTransactionTimeout = defaultTransactionTimeoutMs;
        return cfg;
    }

    /// <summary>
    /// Inserts a key with a write-intent owned by <paramref name="writerTx"/> and a zero-Expires
    /// MVCC snapshot owned by <paramref name="readerTx"/> with the given <paramref name="snapshotExpires"/>.
    /// </summary>
    private static void InsertWithMvcc(
        KeyValueContext context, string key,
        HLCTimestamp writerTx, HLCTimestamp readerTx, HLCTimestamp snapshotExpires)
    {
        HLCTimestamp intentExpires = new(writerTx.N, writerTx.L + 60_000, 0);  // 60 s from now (L is in ms)

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = writerTx,
            LastModified = writerTx,
            Revision = 1,
            FlushedRevision = 1,
            WriteIntent = new KeyValueWriteIntent { TransactionId = writerTx, Expires = intentExpires }
        };
        context.InsertStoreEntry(key, entry);

        entry.MvccEntries = new Dictionary<HLCTimestamp, KeyValueMvccEntry>
        {
            [readerTx] = new()
            {
                State = KeyValueState.Set,
                Revision = 0,
                Value = Encoding.UTF8.GetBytes("snap"),
                Expires = snapshotExpires
            }
        };
        context.AdjustEstimatedEntryBytes(entry,
            KeyValueStoreAccounting.MvccEntryAddedBytes(true, entry.MvccEntries[readerTx].Value));
    }
}

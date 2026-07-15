
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

    /// <summary>
    /// A still-live transaction whose own timeout is larger than the default must not have its snapshot
    /// age-reaped once it passes the default-timeout window: the age bound is the server's <em>maximum</em>
    /// admissible timeout, not the default. Here the reader is older than the default-based window but well
    /// within the maximum-based window, so it is preserved — reaping it would silently drop the participant's
    /// first-committer-wins conflict detection for that transaction.
    /// </summary>
    [Fact]
    public async Task ZeroExpiresSnapshot_PreservedForLiveLongTransactionBeyondDefaultTimeout()
    {
        // Default timeout 5 s (default-based window = 35 s), maximum timeout 60 s (max-based window = 90 s).
        KahunaConfiguration config = CreateConfiguration(defaultTransactionTimeoutMs: 5000, maxTransactionTimeoutMs: 60000);
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // 40 s ago: past the default-based 35 s window (would be wrongly reaped) but within the max-based 90 s.
        HLCTimestamp readerTx = new(now.N, now.L - 40_000, 0);
        HLCTimestamp writerTx = now;

        InsertWithMvcc(context, "snap/key", writerTx, readerTx, HLCTimestamp.Zero);

        // No commit or rollback recorded — a still-live long transaction.

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
            "a live transaction whose larger-than-default timeout has not elapsed must not be age-reaped");
    }

    // ── cold read-only keys: reclaimed by the periodic collector sweep ────────────────────

    /// <summary>
    /// A key read once under a now-dead transaction and never written again is reclaimed by the periodic
    /// collector sweep, without any later commit/rollback/lock-release on that key.
    /// </summary>
    [Fact]
    public void ColdReadOnlyKey_DeadSessionSnapshot_ReclaimedByCollectorSweep()
    {
        (TryCollectHandler collect, KeyValueContext context, RaftManager raft) = CreateCollectHandler();

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp readerTx = new(now.N, now.L - 2000, 0);
        context.RecordCommitted(readerTx);

        InsertReadSnapshotOnly(context, "cold/key", readerTx);

        // No write ever touches the key; only the periodic collector runs.
        collect.Execute();

        KeyValueEntry? entry = context.Store.Get("cold/key");
        Assert.NotNull(entry);
        Assert.True(
            entry!.MvccEntries is null || !entry.MvccEntries.ContainsKey(readerTx),
            "the collector sweep must reclaim a dead-session snapshot on a cold, never-rewritten key");
    }

    /// <summary>
    /// The collector sweep preserves a live transaction's snapshot on a cold key — it is not budget/count
    /// eviction, only dead-session reclamation.
    /// </summary>
    [Fact]
    public void ColdReadOnlyKey_LiveSessionSnapshot_PreservedByCollectorSweep()
    {
        (TryCollectHandler collect, KeyValueContext context, RaftManager raft) = CreateCollectHandler();

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp readerTx = new(now.N, now.L - 500, 0);   // young; no commit/rollback recorded

        InsertReadSnapshotOnly(context, "cold/key", readerTx);

        collect.Execute();

        KeyValueEntry? entry = context.Store.Get("cold/key");
        Assert.NotNull(entry);
        Assert.True(
            entry!.MvccEntries is not null && entry.MvccEntries.ContainsKey(readerTx),
            "a live transaction's snapshot on a cold key must survive the collector sweep");
    }

    /// <summary>
    /// The collector's MVCC sweep is bounded per turn: with a small inspection budget it trims at most that
    /// many entries per cycle and resumes on the next cycle, so a large store is never swept in one mailbox
    /// turn. Successive cycles eventually reclaim every dead-session snapshot.
    /// </summary>
    [Fact]
    public void CollectorMvccSweep_IsBoundedPerTurn_AndResumesUntilComplete()
    {
        (TryCollectHandler collect, KeyValueContext context, RaftManager raft) =
            CreateCollectHandler(CreateConfiguration(collectBatchMax: 2));

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        const int total = 5;
        for (int i = 0; i < total; i++)
        {
            HLCTimestamp readerTx = new(now.N, now.L - (2000 + i), 0);
            context.RecordCommitted(readerTx);
            InsertReadSnapshotOnly(context, $"cold/{i}", readerTx);
        }

        int WithSnapshotRemaining()
        {
            int remaining = 0;
            for (int i = 0; i < total; i++)
            {
                KeyValueEntry? e = context.Store.Get($"cold/{i}");
                if (e?.MvccEntries is { Count: > 0 })
                    remaining++;
            }
            return remaining;
        }

        // One cycle inspects at most the budget (2), so it cannot have reclaimed all five.
        collect.Execute();
        int afterOne = WithSnapshotRemaining();
        Assert.True(afterOne >= total - 2, $"one bounded cycle must not reclaim more than the budget; {afterOne} remain");
        Assert.True(afterOne < total, "one cycle must make progress");

        // Enough cycles complete the sweep.
        for (int c = 0; c < total; c++)
            collect.Execute();
        Assert.Equal(0, WithSnapshotRemaining());
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

    // ── Removing the last MVCC entry nulls the dictionary and conserves byte accounting ──

    /// <summary>
    /// After the only MVCC entry is removed via a lock-release, the dictionary is nulled and
    /// CachedBytes returns exactly to the pre-insert baseline (no double-charge, no leak).
    /// </summary>
    [Fact]
    public async Task LastMvccEntry_RemovedViaRelease_NullsDictionaryAndResetsAccounting()
    {
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp writerTx = now;
        HLCTimestamp readerTx = new(now.N, now.L - 1000, 0);  // committed 1 s ago

        InsertWithMvcc(context, "null/key", writerTx, readerTx, HLCTimestamp.Zero);

        KeyValueEntry entry = context.Store.Get("null/key")!;
        long baselineBytes = entry.CachedBytes
            - KeyValueStoreAccounting.MvccEntryAddedBytes(true, entry.MvccEntries![readerTx].Value);

        context.RecordCommitted(readerTx);

        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "null/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        Assert.Null(entry.MvccEntries);
        Assert.Equal(baselineBytes, entry.CachedBytes);
    }

    /// <summary>
    /// After the only MVCC entry is removed by the expired-entry trim (age-based dead-session
    /// reap), the dictionary is nulled and CachedBytes returns to the pre-insert baseline.
    /// </summary>
    [Fact]
    public async Task LastMvccEntry_RemovedViaTrim_NullsDictionaryAndResetsAccounting()
    {
        KahunaConfiguration config = CreateConfiguration(defaultTransactionTimeoutMs: 5000, maxTransactionTimeoutMs: 5000);
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // An abandoned (age-expired) reader — old enough to be reaped.
        long pastMs = TransactionCoordinator.ReapGraceMs + TransactionCoordinator.MaxParticipantEffectTtlMs + 5000 + 1000;
        HLCTimestamp readerTx = new(now.N, now.L - pastMs, 0);

        // writerTx's own MVCC entry has a non-zero Expires so it stays (we want the trim to only
        // remove the abandoned reader, not the writer's own intent entry).
        HLCTimestamp writerTx = now;
        HLCTimestamp writerExpires = new(writerTx.N, writerTx.L + 60_000, 0);

        // Seed: no writer MVCC entry, just the abandoned reader snapshot.
        KeyValueEntry seedEntry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = writerTx,
            LastModified = writerTx,
            Revision = 1,
            FlushedRevision = 1,
            WriteIntent = new KeyValueWriteIntent { TransactionId = writerTx, Expires = writerExpires }
        };
        context.InsertStoreEntry("trim/key", seedEntry);

        long preInsertBytes = seedEntry.CachedBytes;

        seedEntry.MvccEntries = new Dictionary<HLCTimestamp, KeyValueMvccEntry>
        {
            [readerTx] = new() { State = KeyValueState.Set, Revision = 0, Value = Encoding.UTF8.GetBytes("snap"), Expires = HLCTimestamp.Zero }
        };
        context.AdjustEstimatedEntryBytes(seedEntry, KeyValueStoreAccounting.MvccEntryAddedBytes(true, seedEntry.MvccEntries[readerTx].Value));

        // Trigger: release the writer's lock — writerTx has no MVCC entry, but Trim fires.
        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "trim/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        Assert.Null(seedEntry.MvccEntries);
        Assert.Equal(preInsertBytes, seedEntry.CachedBytes);
    }

    /// <summary>
    /// When two MVCC entries exist and only one is removed, the dictionary stays non-null.
    /// </summary>
    [Fact]
    public async Task PartialMvccRemoval_DictionaryRemainsNonNull()
    {
        KahunaConfiguration config = CreateConfiguration(defaultTransactionTimeoutMs: 5000, maxTransactionTimeoutMs: 5000);
        (TryReleaseExclusiveLockHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp writerTx = now;

        // Dead reader (age-expired) — will be trimmed.
        long pastMs = TransactionCoordinator.ReapGraceMs + TransactionCoordinator.MaxParticipantEffectTtlMs + 5000 + 1000;
        HLCTimestamp deadReader = new(now.N, now.L - pastMs, 0);

        // Live reader (1 s ago) — must survive.
        HLCTimestamp liveReader = new(now.N, now.L - 1000, 0);

        InsertWithMvcc(context, "partial/key", writerTx, deadReader, HLCTimestamp.Zero);

        KeyValueEntry entry = context.Store.Get("partial/key")!;
        // Add a second (live) snapshot.
        entry.MvccEntries![liveReader] = new KeyValueMvccEntry
            { State = KeyValueState.Set, Revision = 0, Value = Encoding.UTF8.GetBytes("live"), Expires = HLCTimestamp.Zero };
        context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(false, entry.MvccEntries[liveReader].Value));

        context.RecordCommitted(writerTx);

        await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            writerTx, HLCTimestamp.Zero, "partial/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        // Dead reader removed, live reader kept.
        Assert.NotNull(entry.MvccEntries);
        Assert.False(entry.MvccEntries.ContainsKey(deadReader), "dead reader must be removed");
        Assert.True(entry.MvccEntries.ContainsKey(liveReader), "live reader must survive");
    }

    /// <summary>
    /// After a committing transaction's own MVCC snapshot — the last one on the entry — is removed by an
    /// ephemeral commit, the dictionary is nulled and CachedBytes returns to the pre-insert baseline. The
    /// snapshot value matches the entry's current value length and carries NoRevision, so the commit's only
    /// accounting change is the MVCC removal.
    /// </summary>
    [Fact]
    public async Task LastMvccEntry_RemovedViaCommit_NullsDictionaryAndResetsAccounting()
    {
        (_, KeyValueContext context, RaftManager raft) = CreateHandler();
        TryCommitMutationsHandler handler = new(context);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp committerTx = now;
        byte[] value = Encoding.UTF8.GetBytes("v");

        KeyValueEntry entry = new()
        {
            Value = value,
            State = KeyValueState.Set,
            LastUsed = committerTx,
            LastModified = committerTx,
            Revision = 1,
            FlushedRevision = 1,
            WriteIntent = new KeyValueWriteIntent { TransactionId = committerTx, Expires = new(committerTx.N, committerTx.L + 60_000, 0) }
        };
        context.InsertStoreEntry("commit/key", entry);

        long preInsertBytes = entry.CachedBytes;

        entry.MvccEntries = new Dictionary<HLCTimestamp, KeyValueMvccEntry>
        {
            [committerTx] = new()
            {
                State = KeyValueState.Set, Revision = 1, NoRevision = true, Value = value,
                Expires = HLCTimestamp.Zero, LastUsed = committerTx, LastModified = committerTx
            }
        };
        context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(true, entry.MvccEntries[committerTx].Value));

        KeyValueResponse response = await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryCommitMutations,
            committerTx, HLCTimestamp.Zero, "commit/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        Assert.Equal(KeyValueResponseType.Committed, response.Type);
        Assert.Null(entry.MvccEntries);
        Assert.Equal(preInsertBytes, entry.CachedBytes);
    }

    /// <summary>
    /// After a rolling-back transaction's own MVCC snapshot — the last one on the entry — is removed by an
    /// ephemeral rollback, the dictionary is nulled and CachedBytes returns to the pre-insert baseline. An
    /// ephemeral rollback touches neither the entry value nor its revisions, so the only accounting change
    /// is the MVCC removal.
    /// </summary>
    [Fact]
    public async Task LastMvccEntry_RemovedViaRollback_NullsDictionaryAndResetsAccounting()
    {
        (_, KeyValueContext context, RaftManager raft) = CreateHandler();
        TryRollbackMutationsHandler handler = new(context);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp rollbackTx = now;

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = rollbackTx,
            LastModified = rollbackTx,
            Revision = 1,
            FlushedRevision = 1,
            WriteIntent = new KeyValueWriteIntent { TransactionId = rollbackTx, Expires = new(rollbackTx.N, rollbackTx.L + 60_000, 0) }
        };
        context.InsertStoreEntry("rollback/key", entry);

        long preInsertBytes = entry.CachedBytes;

        entry.MvccEntries = new Dictionary<HLCTimestamp, KeyValueMvccEntry>
        {
            [rollbackTx] = new()
            {
                State = KeyValueState.Set, Revision = 2, Value = Encoding.UTF8.GetBytes("pending"),
                Expires = new(rollbackTx.N, rollbackTx.L + 60_000, 0)
            }
        };
        context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(true, entry.MvccEntries[rollbackTx].Value));

        KeyValueResponse response = await handler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryRollbackMutations,
            rollbackTx, HLCTimestamp.Zero, "rollback/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        Assert.Equal(KeyValueResponseType.RolledBack, response.Type);
        Assert.Null(entry.MvccEntries);
        Assert.Equal(preInsertBytes, entry.CachedBytes);
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

    private static (TryCollectHandler, KeyValueContext, RaftManager) CreateCollectHandler(
        KahunaConfiguration? config = null)
    {
        config ??= CreateConfiguration();

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "mvcc-reap-collect-test",
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

        return (new TryCollectHandler(context), context, raft);
    }

    private static KahunaConfiguration CreateConfiguration(
        int defaultTransactionTimeoutMs = 5000, int maxTransactionTimeoutMs = 5000, int collectBatchMax = 1000)
    {
        KahunaConfiguration cfg = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
        });
        cfg.DefaultTransactionTimeout = defaultTransactionTimeoutMs;
        cfg.MaxTransactionTimeout = maxTransactionTimeoutMs;
        cfg.CollectBatchMax = collectBatchMax;
        return cfg;
    }

    /// <summary>
    /// Seeds a cold, non-expiring read-only key: a live value with no write intent and a single zero-Expires
    /// MVCC snapshot owned by <paramref name="readerTx"/>. Mirrors a key that was read once under a
    /// transaction and never written again — the case the collector sweep must reclaim.
    /// </summary>
    private static void InsertReadSnapshotOnly(KeyValueContext context, string key, HLCTimestamp readerTx)
    {
        HLCTimestamp now = readerTx;
        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            LastModified = now,
            Revision = 1,
            FlushedRevision = 1
        };
        context.InsertStoreEntry(key, entry);

        entry.MvccEntries = new Dictionary<HLCTimestamp, KeyValueMvccEntry>
        {
            [readerTx] = new()
            {
                State = KeyValueState.Set,
                Revision = 0,
                Value = Encoding.UTF8.GetBytes("snap"),
                Expires = HLCTimestamp.Zero
            }
        };
        context.AdjustEstimatedEntryBytes(entry,
            KeyValueStoreAccounting.MvccEntryAddedBytes(true, entry.MvccEntries[readerTx].Value));
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

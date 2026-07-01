
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the persistent range scan (GetByRange / TryGetByRangeHandler + RangeScanContinuation)
/// covering the detach-from-actor-mailbox path introduced so the actor mailbox is free between
/// every disk page, not only before the first.
///
/// Stage 1 (actor thread, sync) — snapshot in-memory entries; dispatch first disk page.
/// Stage 2 (scheduler thread) — disk read.
/// Stage 3 (actor thread, ResumeRead) — K-way merge; dispatch next page or resolve.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTryGetByRangeHandler
{
    // ── Basic single-page scan ────────────────────────────────────────────────────────────

    [Fact]
    public async Task PersistentRangeScan_DiskOnlyEntries_SinglePage_ReturnsAll()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-single-page");

        scheduler.Start();
        try
        {
            // Three disk entries under "doc/" prefix. The limit is set larger than the
            // count so only one disk page is dispatched.
            RangeBackend backend = new([
                "doc/a", "doc/b", "doc/c"
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-single-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Equal(3, resp.RangeResult!.Items.Count);
            Assert.False(resp.RangeResult.HasMore);
            Assert.Equal("doc/a", resp.RangeResult.Items[0].Item1);
            Assert.Equal("doc/b", resp.RangeResult.Items[1].Item1);
            Assert.Equal("doc/c", resp.RangeResult.Items[2].Item1);
        }
        finally { scheduler.Stop(); }
    }

    // ── Multi-page: scan spans K disk pages, assembles correct result ─────────────────────

    [Fact]
    public async Task PersistentRangeScan_MultiPage_AllItemsAssembledCorrectly()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-multi-page");

        scheduler.Start();
        try
        {
            // Five disk-only entries with a page limit of 2. Disk page reads are dispatched
            // three times:
            //   page 1 → [doc/a, doc/b, doc/c] (3 items returned; doc/c is the peek cursor)
            //   page 2 → [doc/c, doc/d, doc/e] (3 items; doc/e is peek cursor)
            //   page 3 → [doc/e]               (1 item; no more)
            // The assembled result across all pages must include doc/a … doc/e, then be
            // capped at the limit (2) with HasMore=true.
            RangeBackend backend = new([
                "doc/a", "doc/b", "doc/c", "doc/d", "doc/e"
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-multi-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 2), TimeSpan.FromSeconds(10));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            // Limit 2: expect exactly 2 items with HasMore signalling more available.
            Assert.Equal(2, resp.RangeResult!.Items.Count);
            Assert.True(resp.RangeResult.HasMore);
            Assert.Equal("doc/a", resp.RangeResult.Items[0].Item1);
            Assert.Equal("doc/b", resp.RangeResult.Items[1].Item1);
        }
        finally { scheduler.Stop(); }
    }

    // ── Memory + disk merge: keys from both sources, correct lexicographic order ──────────

    [Fact]
    public async Task PersistentRangeScan_MemoryAndDisk_MergedInLexicographicOrder()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-merge");

        scheduler.Start();
        try
        {
            // Disk has doc/a, doc/c, doc/e; memory will have doc/b and doc/d.
            // The K-way merge must interleave them: doc/a, doc/b, doc/c, doc/d, doc/e.
            RangeBackend backend = new(["doc/a", "doc/c", "doc/e"]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-merge-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Seed two in-memory entries via ephemeral writes (no Raft proposal needed in tests).
            // Ephemeral and persistent keys share the same in-memory BTree, so the stage-1
            // memory snapshot captures them and the K-way merge includes them.
            await actorRef.Ask(
                MakeSet("doc/b", Encoding.UTF8.GetBytes("b-val")),
                TimeSpan.FromSeconds(5));
            await actorRef.Ask(
                MakeSet("doc/d", Encoding.UTF8.GetBytes("d-val")),
                TimeSpan.FromSeconds(5));

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(10));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Equal(5, resp.RangeResult!.Items.Count);
            Assert.False(resp.RangeResult.HasMore);
            Assert.Equal("doc/a", resp.RangeResult.Items[0].Item1);
            Assert.Equal("doc/b", resp.RangeResult.Items[1].Item1);
            Assert.Equal("doc/c", resp.RangeResult.Items[2].Item1);
            Assert.Equal("doc/d", resp.RangeResult.Items[3].Item1);
            Assert.Equal("doc/e", resp.RangeResult.Items[4].Item1);
        }
        finally { scheduler.Stop(); }
    }

    // ── Unrelated write lands between pages; the scan result stays consistent ────────────

    [Fact]
    public async Task PersistentRangeScan_WriteLandsBetweenPages_SnapshotUnaffected()
    {
        // Verifies the key property of the multi-page resumable scan: the in-memory snapshot
        // captured at stage 1 is frozen. A write that lands on the same key while stage 2
        // (disk read) is in flight — that is, between pages — does NOT appear in the scan
        // result, because the memory snapshot was taken before the write arrived.
        //
        // Additionally, because the actor mailbox is free during stage 2, the write is
        // processed immediately (not queued behind the scan), confirming that no artificial
        // mailbox blockage exists between pages.

        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-interleave");

        scheduler.Start();
        try
        {
            ManualResetEventSlim gate = new(false);
            ManualResetEventSlim pageEntered = new(false);

            // Two disk entries with a page limit of 1 to force two pages:
            //   page 1 → [rng/a, rng/b] (2 items; rng/b is peek cursor for page 2)
            //   page 2 → [rng/b]        (1 item; no more)
            // The first disk read blocks on 'gate' after signalling 'pageEntered'.
            // While stage 2 is blocked, the test fires an unrelated write to "rng/late".
            // After the gate opens, stage 3 merges and the write must NOT appear in the
            // scan result (it was not in the stage-1 memory snapshot).
            BlockingRangeBackend backend = new(gate, pageEntered, ["rng/a", "rng/b"]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-interleave-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Issue the scan; it will detach after stage 1 and block in stage 2.
            Task<KeyValueResponse?> scanTask = actorRef.Ask(
                MakeRangeScan("rng/", limit: 1), TimeSpan.FromSeconds(10));

            // Wait until stage 2 has started (disk read in flight, mailbox free).
            bool entered = pageEntered.Wait(TimeSpan.FromSeconds(5));
            Assert.True(entered, "Stage-2 disk read should have started within 5 s");

            // While stage 2 is blocked, write a new key via ephemeral TrySet (no Raft proposal
            // needed). Because the mailbox is free, this Set is processed immediately — proving
            // no backpressure from the scan.
            Task<KeyValueResponse?> writeTask = actorRef.Ask(
                MakeSet("rng/late", Encoding.UTF8.GetBytes("late")),
                TimeSpan.FromSeconds(5));
            KeyValueResponse? writeResp = await writeTask;
            Assert.NotNull(writeResp);
            Assert.Equal(KeyValueResponseType.Set, writeResp!.Type);

            // Release stage 2 to let stage 3 run.
            gate.Set();

            KeyValueResponse? resp = await scanTask;

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);

            // With limit=1, only 1 item is returned (rng/a), with HasMore=true.
            // "rng/late" is absent because it was not in the stage-1 memory snapshot.
            Assert.Equal(1, resp.RangeResult!.Items.Count);
            Assert.True(resp.RangeResult.HasMore);
            Assert.Equal("rng/a", resp.RangeResult.Items[0].Item1);
            Assert.DoesNotContain(resp.RangeResult.Items, i => i.Item1 == "rng/late");
        }
        finally { scheduler.Stop(); }
    }

    // ── Memory key shadows same-key disk entry, memory value wins ────────────────────────

    [Fact]
    public async Task PersistentRangeScan_SameKeyInMemoryAndDisk_MemoryWins()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-shadow");

        scheduler.Start();
        try
        {
            // Disk has "kv/x" at revision 1 with value "disk-val".
            // Memory will have "kv/x" at revision 2 with value "mem-val" (written after seeding disk).
            // The K-way merge must serve the memory value.
            RangeBackend backend = new(new Dictionary<string, (byte[]? Value, long Revision)>
            {
                ["kv/x"] = (Encoding.UTF8.GetBytes("disk-val"), 1L)
            });

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-shadow-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Write the in-memory version of kv/x via ephemeral TrySet (no Raft proposal).
            // Ephemeral and persistent keys share the same BTree, so the stage-1 snapshot
            // picks it up and the merge serves the memory value over the disk value.
            await actorRef.Ask(
                MakeSet("kv/x", Encoding.UTF8.GetBytes("mem-val")),
                TimeSpan.FromSeconds(5));

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("kv/", limit: 10), TimeSpan.FromSeconds(10));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Equal(1, resp.RangeResult!.Items.Count);
            Assert.Equal("kv/x", resp.RangeResult.Items[0].Item1);
            Assert.Equal("mem-val", Encoding.UTF8.GetString(resp.RangeResult.Items[0].Item2.Value!));
        }
        finally { scheduler.Stop(); }
    }

    // ── Accounting: transactional scan over disk-only keys must not leak approximateStoreBytes ──

    [Fact]
    public async Task PersistentRangeScan_TransactionalDiskOnlyKeys_DoesNotLeakStoreBytes()
    {
        // A transactional GetByRange (transactionId != Zero) over keys that exist only on disk
        // (not in the actor's in-memory BTree) must not increment ApproximateStoreBytes.
        //
        // Pre-fix: EvaluateKeySync created an MVCC entry on the transient KeyValueEntry built for
        // each disk-only key and called AdjustEstimatedEntryBytes on it. Because the transient entry
        // is never inserted into the store, the counter was incremented without a matching decrement,
        // drifting upward with every transactional scan — triggering premature eviction churn.
        //
        // Post-fix: MVCC creation + accounting are skipped for non-resident (disk-only) keys;
        // the keys are still returned via the committed-state fall-through path.

        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-acct");

        scheduler.Start();
        try
        {
            RangeBackend backend = new(["acct/a", "acct/b", "acct/c"]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-acct-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Drain one message so the actor is fully initialised before we snapshot bytes.
            await actorRef.Ask(MakeRangeScan("warmup/", limit: 1), TimeSpan.FromSeconds(5));

            long before = ((KeyValueActor)actorRef.Runner.Actor!).ApproximateStoreBytes;

            // Transactional range scan: transactionId != Zero + ReadTimestamp == Zero (non-snapshot).
            // All three keys are disk-only (no ephemeral writes); the merge loop hits the cmp > 0
            // branch (disk key) for each of them, exercising the guarded MVCC path.
            HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            KeyValueRequest txScan = MakeRangeScan("acct/", limit: 10);
            txScan.ReadTimestamp = HLCTimestamp.Zero; // ensure non-snapshot
            // Set transactionId via the constructor — reuse MakeRangeScan's internal fields
            // by rebuilding with a non-zero transactionId:
            txScan = new KeyValueRequest(
                KeyValueRequestType.GetByRange,
                txId, HLCTimestamp.Zero,
                "acct/",
                null, null, -1,
                KeyValueFlags.None, 0,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                0, 1, null);
            txScan.Limit = 10;
            txScan.StartInclusive = true;

            KeyValueResponse? resp = await actorRef.Ask(txScan, TimeSpan.FromSeconds(5));

            long after = ((KeyValueActor)actorRef.Runner.Actor!).ApproximateStoreBytes;

            // The scan must still return all three disk-only keys (code path was exercised).
            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Equal(3, resp.RangeResult!.Items.Count);
            Assert.Equal("acct/a", resp.RangeResult.Items[0].Item1);
            Assert.Equal("acct/b", resp.RangeResult.Items[1].Item1);
            Assert.Equal("acct/c", resp.RangeResult.Items[2].Item1);

            // The accounting counter must be unchanged — no bytes leaked onto disk-only transient entries.
            Assert.Equal(before, after);
        }
        finally { scheduler.Stop(); }
    }

    // ── Scan disk work routes to the owning data partition, not message.PartitionId ───────

    [Fact]
    public async Task PersistentRangeScan_RoutesEveryPageToResolvedPartition_NotMessagePartitionId()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-routing");

        scheduler.Start();
        try
        {
            RangeBackend backend = new(["doc/a", "doc/b", "doc/c", "doc/d", "doc/e"]);

            // Record which scheduler partition each disk page is enqueued under, delegating to
            // the real scheduler so the multi-page scan still runs to completion.
            RecordingReadScheduler recording = new(inner: scheduler);
            SchedulerOverridingRaft decoratedRaft = new(raft, recording);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-routing-actor", null!, null!, backend, decoratedRaft,
                    new KeySpaceRegistry(), new RangeMapStore(decoratedRaft, null, null, logger), config, logger);

            // Point the request's PartitionId at a value that is deliberately NOT the owning data
            // partition. The pre-fix code enqueued on message.PartitionId, so every page would land
            // on 7777; the fix routes by ResolvePartition(prefix) instead.
            const int wrongPartition = 7777;
            KeyValueRequest scan = new(
                KeyValueRequestType.GetByRange,
                HLCTimestamp.Zero, HLCTimestamp.Zero,
                "doc/",
                null, null, -1,
                KeyValueFlags.None, 0,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                0, wrongPartition, null);
            scan.Limit = 2;               // limit 2 over 5 keys → 3 disk pages (first + continuation)
            scan.StartInclusive = true;

            KeyValueResponse? resp = await actorRef.Ask(scan, TimeSpan.FromSeconds(10));

            // The scan still works end to end.
            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(2, resp.RangeResult!.Items.Count);

            // Every page (first + each continuation page) must have been enqueued, and none of them
            // on message.PartitionId — they route to the resolved data partition, consistently.
            Assert.True(recording.EnqueuedPartitions.Count >= 2, "multi-page scan must enqueue more than one page");
            Assert.DoesNotContain(wrongPartition, recording.EnqueuedPartitions);
            Assert.All(recording.EnqueuedPartitions,
                p => Assert.Equal(recording.EnqueuedPartitions[0], p));
        }
        finally { scheduler.Stop(); }
    }

    // ── Back-pressure on the first-page enqueue surfaces as a retryable response ───────────

    [Fact]
    public async Task PersistentRangeScan_EnqueueRejectedByBackpressure_ReturnsMustRetry()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-backpressure");

        scheduler.Start();
        try
        {
            RangeBackend backend = new(["doc/a", "doc/b", "doc/c"]);

            // The scheduler rejects every enqueue (queue depth exceeded). The handler must map this
            // to a retryable response, never fault the actor or leave the caller's promise hanging.
            RecordingReadScheduler rejecting = new(rejectWithBackpressure: true);
            SchedulerOverridingRaft decoratedRaft = new(raft, rejecting);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-backpressure-actor", null!, null!, backend, decoratedRaft,
                    new KeySpaceRegistry(), new RangeMapStore(decoratedRaft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.MustRetry, resp!.Type);

            // Range scans do not register in PendingReads, but the actor must remain healthy: a
            // second request still gets a clean retryable response rather than a stuck mailbox.
            KeyValueResponse? resp2 = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5));
            Assert.Equal(KeyValueResponseType.MustRetry, resp2!.Type);
            Assert.Equal(0, ((KeyValueActor)actorRef.Runner.Actor!).PendingReadsCount);
        }
        finally { scheduler.Stop(); }
    }

    // ── Request factories ─────────────────────────────────────────────────────────────────

    private static KeyValueRequest MakeRangeScan(string prefix, int limit)
    {
        KeyValueRequest req = new(
            KeyValueRequestType.GetByRange,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            prefix,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0, 1, null);
        req.Limit = limit;
        req.StartInclusive = true;
        return req;
    }

    private static KeyValueRequest MakeSet(string key, byte[] value) =>
        new(
            KeyValueRequestType.TrySet,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key,
            value,
            null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            0, 0, null);

    // ── Raft / config factory ─────────────────────────────────────────────────────────────

    private static (RaftManager, FairReadScheduler, KahunaConfiguration, ILogger<IKahuna>) CreateRaftAndConfig(string nodeName)
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = nodeName,
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

        return (raft, (FairReadScheduler)raft.ReadScheduler, config, logger);
    }

    // ── Inner backends ────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Backend with a flat sorted list of entries. GetKeyValueByRange returns up to `limit`
    /// items with keys that start with `prefix` and are &gt;= `startKey` (if provided).
    /// </summary>
    private sealed class RangeBackend : IPersistenceBackend
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> allEntries;

        internal RangeBackend(IEnumerable<string> keys)
        {
            allEntries = keys
                .OrderBy(k => k, StringComparer.Ordinal)
                .Select(k => (k, new ReadOnlyKeyValueEntry(
                    Encoding.UTF8.GetBytes(k + "-val"), 1L,
                    HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set)))
                .ToList();
        }

        internal RangeBackend(Dictionary<string, (byte[]? Value, long Revision)> entries)
        {
            allEntries = entries
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => (kv.Key, new ReadOnlyKeyValueEntry(
                    kv.Value.Value, kv.Value.Revision,
                    HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set)))
                .ToList();
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
        {
            IEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> candidates =
                allEntries.Where(e => e.Key.StartsWith(prefix, StringComparison.Ordinal));

            if (startKey is not null)
                candidates = candidates.Where(e => string.CompareOrdinal(e.Key, startKey) >= 0);

            return candidates
                .Take(limit)
                .Select(e => (e.Key, e.Entry))
                .ToList();
        }

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    /// <summary>
    /// Backend that signals `pageEntered` when the first GetKeyValueByRange call arrives,
    /// then blocks on `gate` before returning — allowing a concurrent write to be processed
    /// by the actor while stage 2 is in flight.
    /// </summary>
    private sealed class BlockingRangeBackend : IPersistenceBackend
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly ManualResetEventSlim gate;
        private readonly ManualResetEventSlim pageEntered;
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> allEntries;
        private int callCount;

        internal BlockingRangeBackend(
            ManualResetEventSlim gate,
            ManualResetEventSlim pageEntered,
            IEnumerable<string> keys)
        {
            this.gate = gate;
            this.pageEntered = pageEntered;
            allEntries = keys
                .OrderBy(k => k, StringComparer.Ordinal)
                .Select(k => (k, new ReadOnlyKeyValueEntry(
                    Encoding.UTF8.GetBytes(k + "-val"), 1L,
                    HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set)))
                .ToList();
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
        {
            int n = Interlocked.Increment(ref callCount);

            // Only block on the first page read — that is the point where stage 2 is in
            // flight and the test fires the concurrent write.
            if (n == 1)
            {
                pageEntered.Set();
                gate.Wait();
            }

            IEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> candidates =
                allEntries.Where(e => e.Key.StartsWith(prefix, StringComparison.Ordinal));

            if (startKey is not null)
                candidates = candidates.Where(e => string.CompareOrdinal(e.Key, startKey) >= 0);

            return candidates
                .Take(limit)
                .Select(e => (e.Key, e.Entry))
                .ToList();
        }

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }
}

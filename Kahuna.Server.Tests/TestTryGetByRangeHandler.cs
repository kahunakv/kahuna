
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
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
public sealed class TestTryGetByRangeHandler : RaftTrackingTest
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

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-single-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

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
            // Five disk-only entries with a page limit of 2. The first disk page read returns
            // [doc/a, doc/b, doc/c]: doc/c is the limit+1 sentinel, merged like any row, so the
            // merge holds three items after one read and knows more rows exist. The response is
            // capped at the limit (2) with HasMore=true and the cursor at doc/b.
            RangeBackend backend = new([
                "doc/a", "doc/b", "doc/c", "doc/d", "doc/e"
            ]);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-multi-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 2), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

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

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-merge-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Seed two in-memory entries via ephemeral writes (no Raft proposal needed in tests).
            // Ephemeral and persistent keys share the same in-memory BTree, so the stage-1
            // memory snapshot captures them and the K-way merge includes them.
            await actorRef.Ask(
                MakeSet("doc/b", Encoding.UTF8.GetBytes("b-val")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await actorRef.Ask(
                MakeSet("doc/d", Encoding.UTF8.GetBytes("d-val")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

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

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-interleave-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Issue the scan; it will detach after stage 1 and block in stage 2.
            Task<KeyValueResponse?> scanTask = actorRef.Ask(
                MakeRangeScan("rng/", limit: 1), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // Wait until stage 2 has started (disk read in flight, mailbox free).
            bool entered = pageEntered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.True(entered, "Stage-2 disk read should have started within 5 s");

            // While stage 2 is blocked, write a new key via ephemeral TrySet (no Raft proposal
            // needed). Because the mailbox is free, this Set is processed immediately — proving
            // no backpressure from the scan.
            Task<KeyValueResponse?> writeTask = actorRef.Ask(
                MakeSet("rng/late", Encoding.UTF8.GetBytes("late")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
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
            Assert.Single(resp.RangeResult!.Items);
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

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-shadow-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Write the in-memory version of kv/x via ephemeral TrySet (no Raft proposal).
            // Ephemeral and persistent keys share the same BTree, so the stage-1 snapshot
            // picks it up. Three sets advance the resident entry to revision 2 — past the disk
            // row's revision 1 — so the revision-aware collision serves the memory value.
            await actorRef.Ask(
                MakeSet("kv/x", Encoding.UTF8.GetBytes("mem-val-0")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await actorRef.Ask(
                MakeSet("kv/x", Encoding.UTF8.GetBytes("mem-val-1")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await actorRef.Ask(
                MakeSet("kv/x", Encoding.UTF8.GetBytes("mem-val")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("kv/", limit: 10), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Single(resp.RangeResult!.Items);
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

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-acct-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Drain one message so the actor is fully initialised before we snapshot bytes.
            await actorRef.Ask(MakeRangeScan("warmup/", limit: 1), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

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
                0, 1, default);
            txScan.Limit = 10;
            txScan.StartInclusive = true;

            KeyValueResponse? resp = await actorRef.Ask(txScan, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

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
            // The first three keys are tombstones on disk: the first page (limit + 1 = 3 rows) yields
            // no live row, so the merge must fetch continuation pages to fill the result.
            RangeBackend backend = new(
                ["doc/a", "doc/b", "doc/c", "doc/d", "doc/e"],
                k => k is "doc/a" or "doc/b" or "doc/c" ? KeyValueState.Deleted : KeyValueState.Set);

            // Record which scheduler partition each disk page is enqueued under, delegating to
            // the real scheduler so the multi-page scan still runs to completion.
            RecordingReadScheduler recording = new(inner: scheduler);
            SchedulerOverridingRaft decoratedRaft = new(raft, recording);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-routing-actor", null!, null!, backend, decoratedRaft,
                    decoratedRaft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(decoratedRaft, null, null, logger), config, logger);

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
                0, wrongPartition, default);
            scan.Limit = 2;               // limit 2 over 3 dead + 2 live keys → first page plus continuation pages
            scan.StartInclusive = true;

            KeyValueResponse? resp = await actorRef.Ask(scan, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // The scan still works end to end.
            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(["doc/d", "doc/e"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());

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

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-backpressure-actor", null!, null!, backend, decoratedRaft,
                    decoratedRaft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(decoratedRaft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.MustRetry, resp!.Type);

            // Range scans do not register in PendingReads, but the actor must remain healthy: a
            // second request still gets a clean retryable response rather than a stuck mailbox.
            KeyValueResponse? resp2 = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.MustRetry, resp2!.Type);
            Assert.Equal(0, ((KeyValueActor)actorRef.Runner.Actor!).PendingReadsCount);
        }
        finally { scheduler.Stop(); }
    }

    // ── In-memory backend: the page is read and merged in the actor turn, with no scheduler hand-off ──

    /// <summary>
    /// A backend that serves reads from memory answers a page inside the turn that received the request:
    /// the backend read runs on the actor thread, the merge follows at once, and the reply is synchronous.
    /// Nothing is enqueued on the backend read scheduler, and the resident keys still merge with the page.
    /// </summary>
    [Fact]
    public async Task PersistentRangeScan_InMemoryBackend_PageReadAndMergedInTheActorTurn_NoSchedulerHandOff()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-inline-page");

        scheduler.Start();
        try
        {
            RangeBackend backend = new(["doc/a", "doc/b", "doc/c"], servesReadsFromMemory: true);

            // No inner scheduler: an enqueue would still run (off the caller's thread) and be recorded.
            RecordingReadScheduler recording = new();

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-inline-page-actor", null!, null!, backend, raft,
                    recording, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            await actorRef.Ask(MakeSet("doc/bb", Encoding.UTF8.GetBytes("bb-val")), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(["doc/a", "doc/b", "doc/bb", "doc/c"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.False(resp.RangeResult.HasMore);
            Assert.True(resp.RangeResult.Observation!.Value.DiskFallback);

            Assert.Empty(recording.EnqueuedPartitions);
            PageRead read = Assert.Single(backend.Reads);
            Assert.Equal(11, read.Limit);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A page continued from a cursor starts at the last returned key, excluded, and the backend seek is inclusive,
    /// so the cursor row takes one slot of the page. The page is read one row longer for it and closes on one
    /// backend read; before, every cursor page read a second page to find its sentinel. Holds on both page paths:
    /// on the scheduler, where each page is exactly one hand-off, and on the actor thread, where it is none.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PersistentRangeScan_CursorPages_OneBackendReadPerPage(bool servesReadsFromMemory)
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-cursor-pages-" + servesReadsFromMemory);

        scheduler.Start();
        try
        {
            string[] diskKeys = HundredDocKeys();
            string[] residentKeys = ["doc/005a", "doc/050a", "doc/099a"];

            RangeBackend backend = new(diskKeys, servesReadsFromMemory: servesReadsFromMemory);
            RecordingReadScheduler recording = new(inner: scheduler);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-cursor-pages-actor", null!, null!, backend, raft,
                    recording, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            foreach (string key in residentKeys)
                await actorRef.Ask(MakeSet(key, Encoding.UTF8.GetBytes(key + "-mem")), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            const int limit = 7;
            (List<string> keys, int pages) = await WalkByCursor(actorRef, "doc/", limit);

            List<string> union = diskKeys.Concat(residentKeys).OrderBy(static k => k, StringComparer.Ordinal).ToList();
            Assert.Equal(union, keys);
            Assert.Equal((union.Count + limit - 1) / limit, pages);

            List<PageRead> reads = backend.Reads;
            Assert.Equal(pages, reads.Count);
            Assert.Equal(limit + 1, reads[0].Limit);
            Assert.All(reads.Skip(1), static r => Assert.Equal(limit + 2, r.Limit));

            Assert.Equal(servesReadsFromMemory ? 0 : pages, recording.EnqueuedPartitions.Count);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// The inline walk is bounded like the ephemeral scan: a range dense with dead rows is read on the actor
    /// thread up to the inspection budget, and the rest of the scan takes the scheduler path, where the mailbox
    /// is free between pages. The result is the same either way.
    /// </summary>
    [Fact]
    public async Task PersistentRangeScan_InMemoryBackend_DeadRowsPastTheInspectionBudget_HandTheRestToTheScheduler()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-inline-budget");

        scheduler.Start();
        try
        {
            const int deadRows = 9_000;
            const int limit = 100;

            List<string> keys = Enumerable.Range(0, deadRows).Select(static i => $"doc/d{i:D5}").ToList();
            keys.Add("doc/live1");
            keys.Add("doc/live2");

            RangeBackend backend = new(keys, k => k.StartsWith("doc/d", StringComparison.Ordinal) ? KeyValueState.Deleted : KeyValueState.Set, servesReadsFromMemory: true);
            RecordingReadScheduler recording = new(inner: scheduler);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-inline-budget-actor", null!, null!, backend, raft,
                    recording, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit), TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(["doc/live1", "doc/live2"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.False(resp.RangeResult.HasMore);
            Assert.Equal(deadRows + 2, resp.RangeResult.Observation!.Value.EntriesInspected);

            // Each page merges limit + 1 new rows; the walk stays inline until the inspected count reaches
            // the budget, then every further page is one scheduler hand-off.
            int inlinePages = (limit + 1 + KeyValueScanLimits.MaxScanInspectionSlack + limit) / (limit + 1);
            int scheduledPages = recording.EnqueuedPartitions.Count;
            Assert.True(scheduledPages >= 1, "the walk past the budget must take the scheduler path");
            Assert.Equal(inlinePages + scheduledPages, backend.Reads.Count);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// The production backend is the unflushed-writes overlay over the in-memory backend. The overlay forwards
    /// the in-memory answer, so the page is read on the actor thread, and a committed write that is still
    /// queued for the background writer is visible in that page: read-after-write holds without any hand-off.
    /// </summary>
    [Fact]
    public async Task PersistentRangeScan_OverlayOverInMemoryBackend_UnflushedWriteVisibleInTheActorTurn()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-inline-overlay");

        scheduler.Start();
        try
        {
            MemoryPersistenceBackend memory = new();
            memory.StoreKeyValues(
            [
                FlushedRow("doc/a", "a-disk"),
                FlushedRow("doc/c", "c-disk")
            ]);

            UnflushedKeyValueWritesIndex unflushed = new();
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-unflushed"), 1, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 2000, 0), KeyValueState.Set, false);

            UnflushedOverlayPersistenceBackend overlay = new(memory, unflushed, new UnflushedLockWritesIndex());
            Assert.True(overlay.ServesReadsFromMemory);

            RecordingReadScheduler recording = new();

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-inline-overlay-actor", null!, null!, overlay, raft,
                    recording, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            await actorRef.Ask(MakeSet("doc/d", Encoding.UTF8.GetBytes("d-mem")), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(["doc/a", "doc/b", "doc/c", "doc/d"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.Equal("b-unflushed", Encoding.UTF8.GetString(resp.RangeResult.Items[1].Item2.Value!));
            Assert.False(resp.RangeResult.HasMore);
            Assert.Empty(recording.EnqueuedPartitions);
        }
        finally { scheduler.Stop(); }
    }

    private static PersistenceRequestItem FlushedRow(string key, string value, long revision = 1, long physical = 1000) =>
        new(
            key: key,
            value: Encoding.UTF8.GetBytes(value),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: physical, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: physical, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    /// <summary>Walks every page of a scan as the scan loop does: each next page starts after the last returned key.</summary>
    private static async Task<(List<string> Keys, int Pages)> WalkByCursor(
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef, string prefix, int limit)
    {
        List<string> keys = [];
        int pages = 0;
        string? cursor = null;

        while (true)
        {
            KeyValueRequest request = cursor is null
                ? MakeRangeScan(prefix, limit)
                : MakeBoundedRangeScan(prefix, limit, cursor, false, null, false);

            KeyValueResponse? resp = await actorRef.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            pages++;

            foreach ((string key, ReadOnlyKeyValueEntry _) in resp.RangeResult!.Items)
                keys.Add(key);

            if (!resp.RangeResult.HasMore)
                return (keys, pages);

            Assert.True(KeyValueRangeCursor.TryDecode(resp.RangeResult.NextCursor!, out string lastKey, out _, out _, out _));
            cursor = lastKey;
        }
    }

    // ── End bound reaches the backend: a bounded seek reads only the entries inside it ────

    private static string[] HundredDocKeys() => Enumerable.Range(0, 100).Select(static i => $"doc/{i:D3}").ToArray();

    private async Task<(KeyValueResponse Response, RangeBackend Backend)> RunBoundedScan(
        string nodeName, string[] diskKeys, int limit, string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig(nodeName);

        scheduler.Start();
        try
        {
            RangeBackend backend = new(diskKeys);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    nodeName + "-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeBoundedRangeScan("doc/", limit, startKey, startInclusive, endKey, endInclusive),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            return (resp!, backend);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A bounded seek over a large range used to read a full limit+1 page from the backend and discard
    /// everything past the bound in the merge. The backend must now receive the bound and return only
    /// the entries inside it, and the page must close without a second read.
    /// </summary>
    [Fact]
    public async Task PersistentRangeScan_EndKeyInsideFirstPage_BackendReadsOnlyTheBoundedEntries()
    {
        (KeyValueResponse resp, RangeBackend backend) = await RunBoundedScan(
            "range-bound-inside", HundredDocKeys(), limit: 50, "doc/010", true, "doc/015", false);

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(["doc/010", "doc/011", "doc/012", "doc/013", "doc/014"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
        Assert.False(resp.RangeResult.HasMore);

        PageRead read = Assert.Single(backend.Reads);
        Assert.Equal("doc/010", read.StartKey);
        Assert.Equal("doc/015", read.EndKey);
        Assert.False(read.EndInclusive);
        Assert.Equal(51, read.Limit);
        Assert.Equal(5, read.Returned);
    }

    /// <summary>
    /// A bound that ends exactly where the page ends: the raw page is `limit` entries, not the
    /// `limit + 1` sentinel, so the handler knows the range is exhausted without reading a second page.
    /// </summary>
    [Theory]
    [InlineData("doc/009", true)]
    [InlineData("doc/010", false)]
    public async Task PersistentRangeScan_EndKeyAtPageEnd_ClosesWithoutASecondRead(string endKey, bool endInclusive)
    {
        (KeyValueResponse resp, RangeBackend backend) = await RunBoundedScan(
            "range-bound-page-end", HundredDocKeys(), limit: 10, "doc/000", true, endKey, endInclusive);

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(Enumerable.Range(0, 10).Select(static i => $"doc/{i:D3}").ToList(), resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
        Assert.False(resp.RangeResult.HasMore);

        PageRead read = Assert.Single(backend.Reads);
        Assert.Equal(10, read.Returned);
    }

    /// <summary>A bound past the last key of the prefix: the prefix ends the page, as before.</summary>
    [Fact]
    public async Task PersistentRangeScan_EndKeyPastLastKeyOfPrefix_ReturnsThePrefixTail()
    {
        (KeyValueResponse resp, RangeBackend backend) = await RunBoundedScan(
            "range-bound-past-prefix", HundredDocKeys(), limit: 10, "doc/095", true, "doc/zzz", true);

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(["doc/095", "doc/096", "doc/097", "doc/098", "doc/099"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
        Assert.False(resp.RangeResult.HasMore);

        PageRead read = Assert.Single(backend.Reads);
        Assert.Equal(5, read.Returned);
    }

    /// <summary>
    /// A bound wider than one page: the first page carries the sentinel and a cursor, and the sentinel
    /// row itself tells the merge that more rows exist, so the page closes on one backend read. The
    /// continuation page from that cursor stops at the bound with no further read.
    /// </summary>
    [Fact]
    public async Task PersistentRangeScan_EndKeyBeyondOnePage_PaginatesThenStopsAtTheBound()
    {
        (KeyValueResponse first, RangeBackend firstBackend) = await RunBoundedScan(
            "range-bound-two-pages-1", HundredDocKeys(), limit: 4, "doc/000", true, "doc/006", false);

        Assert.Equal(KeyValueResponseType.Get, first.Type);
        Assert.Equal(["doc/000", "doc/001", "doc/002", "doc/003"], first.RangeResult!.Items.Select(static i => i.Item1).ToList());
        Assert.True(first.RangeResult.HasMore);
        PageRead firstRead = Assert.Single(firstBackend.Reads);
        Assert.Equal(5, firstRead.Returned); // limit + 1 inside the bound: the sentinel

        // The client's next page starts strictly after the last returned key, with the same bound.
        (KeyValueResponse second, RangeBackend secondBackend) = await RunBoundedScan(
            "range-bound-two-pages-2", HundredDocKeys(), limit: 4, "doc/003", false, "doc/006", false);

        Assert.Equal(KeyValueResponseType.Get, second.Type);
        Assert.Equal(["doc/004", "doc/005"], second.RangeResult!.Items.Select(static i => i.Item1).ToList());
        Assert.False(second.RangeResult.HasMore);
        PageRead secondRead = Assert.Single(secondBackend.Reads);
        Assert.Equal(3, secondRead.Returned); // doc/003 (the exclusive start, seeked inclusively), doc/004, doc/005
    }

    /// <summary>
    /// Differential check of the bounded path over resident and disk keys: for random start and end bounds,
    /// inclusive flags and limits, the page must equal the ordinal union of both sources trimmed to the bounds
    /// and cut at the limit, with HasMore exactly when more keys remain inside the bounds. Both page paths:
    /// read on the scheduler, and read on the actor thread as for an in-memory backend.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PersistentRangeScan_WithEndKey_MatchesTheOrderedUnionTrimmedAtTheBounds(bool servesReadsFromMemory)
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-bound-differential-" + servesReadsFromMemory);

        scheduler.Start();
        try
        {
            Random random = new(1234);
            string[] universe = HundredDocKeys();
            List<string> diskKeys = universe.Where(_ => random.Next(2) == 0).ToList();
            List<string> residentKeys = universe.Where(_ => random.Next(3) == 0).ToList();

            RangeBackend backend = new(diskKeys, servesReadsFromMemory: servesReadsFromMemory);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-bound-differential-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            foreach (string key in residentKeys)
                await actorRef.Ask(MakeSet(key, Encoding.UTF8.GetBytes(key + "-mem")), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            List<string> union = diskKeys.Union(residentKeys).OrderBy(static k => k, StringComparer.Ordinal).ToList();

            for (int round = 0; round < 60; round++)
            {
                string? startKey = random.Next(4) == 0 ? null : $"doc/{random.Next(0, 100):D3}";
                bool startInclusive = random.Next(2) == 0;
                string? endKey = random.Next(5) == 0 ? null : $"doc/{random.Next(0, 100):D3}";
                bool endInclusive = random.Next(2) == 0;
                int limit = random.Next(5) switch { 0 => 1, 1 => 3, 2 => 7, 3 => 50, _ => 200 };

                List<string> inBounds = union.Where(k =>
                    (startKey is null || string.CompareOrdinal(k, startKey) > 0 || (startInclusive && string.CompareOrdinal(k, startKey) == 0))
                    && (endKey is null || IPersistenceBackend.IsWithinEndBound(k, endKey, endInclusive))).ToList();

                KeyValueResponse? resp = await actorRef.Ask(
                    MakeBoundedRangeScan("doc/", limit, startKey, startInclusive, endKey, endInclusive),
                    TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                Assert.NotNull(resp);
                Assert.Equal(KeyValueResponseType.Get, resp!.Type);
                Assert.Equal(inBounds.Take(limit).ToList(), resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
                Assert.Equal(inBounds.Count > limit, resp.RangeResult.HasMore);
            }
        }
        finally { scheduler.Stop(); }
    }

    // ── Request factories ─────────────────────────────────────────────────────────────────

    private static KeyValueRequest MakeBoundedRangeScan(
        string prefix, int limit, string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        KeyValueRequest req = MakeRangeScan(prefix, limit);
        req.StartKey = startKey;
        req.StartInclusive = startInclusive;
        req.EndKey = endKey;
        req.EndInclusive = endInclusive;
        return req;
    }

    // ── Queued history below a head past the snapshot: the projection fences itself ─────────

    /// <summary>
    /// A key this actor never held resident (a follower applies to the backend only, and its rows reach a
    /// new leader this way) has its first revision still queued for the background writer when a later commit
    /// moves its head past the snapshot. The as-of projection must then not answer from the persisted history,
    /// which is missing the queued revision: it would drop the row, and the scan would count one row fewer than
    /// the snapshot held. With no resident entry to answer for the key, the page answers MustRetry instead, and
    /// once the flush lands the row is served.
    /// </summary>
    [Fact]
    public async Task SnapshotRangeScan_QueuedFirstRevisionBelowAHeadPastTheSnapshot_RetriesUntilTheFlushLands()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-queued-history-fence");

        scheduler.Start();
        try
        {
            HLCTimestamp firstRevisionTs = new(0, 1500, 0);
            HLCTimestamp snapshotTs      = new(0, 2000, 0);
            HLCTimestamp deleteTs        = new(0, 2500, 0);

            MemoryPersistenceBackend memory = new();
            memory.StoreKeyValues(
            [
                FlushedRow("doc/a", "a-disk"),
                FlushedRow("doc/c", "c-disk")
            ]);

            // doc/b: inserted before the snapshot and deleted after it, both still queued for the writer. The
            // overlay shows the head (the tombstone) and the persisted history holds nothing for the key.
            UnflushedKeyValueWritesIndex unflushed = new();
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v0"), 0, HLCTimestamp.Zero, firstRevisionTs, firstRevisionTs, KeyValueState.Set, false);
            unflushed.Record("doc/b", null, 1, HLCTimestamp.Zero, deleteTs, deleteTs, KeyValueState.Deleted, false);

            UnflushedOverlayPersistenceBackend overlay = new(memory, unflushed, new UnflushedLockWritesIndex());

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-queued-history-fence-actor", null!, null!, overlay, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? fenced = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, snapshotTs), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(fenced);
            Assert.Equal(KeyValueResponseType.MustRetry, fenced!.Type);

            // The flush of the first revision lands: the history can answer for the key below its queued head.
            memory.StoreKeyValues(
            [
                new PersistenceRequestItem(
                    key: "doc/b",
                    value: Encoding.UTF8.GetBytes("b-v0"),
                    revision: 0,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 1500, lastUsedCounter: 0,
                    lastModifiedNode: 0, lastModifiedPhysical: 1500, lastModifiedCounter: 0,
                    state: (int)KeyValueState.Set)
            ]);
            unflushed.RemoveFlushed("doc/b", 0, firstRevisionTs);

            KeyValueResponse? served = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, snapshotTs), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(served);
            Assert.Equal(KeyValueResponseType.Get, served!.Type);
            Assert.Equal(["doc/a", "doc/b", "doc/c"], served.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.Equal(0L, served.RangeResult.Items[1].Item2.Revision);
            Assert.Equal("b-v0", Encoding.UTF8.GetString(served.RangeResult.Items[1].Item2.Value!));
            Assert.False(served.RangeResult.HasMore);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// The stage-2 projection alone: a row whose head is past the snapshot and whose older revision is still
    /// queued is left out of the page and named as fenced instead of being projected from the incomplete
    /// history; a row whose only queued revision is the head itself is projected from the history as before.
    /// </summary>
    [Fact]
    public void ProjectSnapshotPage_QueuedRevisionBelowTheHead_LeavesTheRowToTheMerge()
    {
        HLCTimestamp firstRevisionTs = new(0, 1500, 0);
        HLCTimestamp snapshotTs      = new(0, 2000, 0);
        HLCTimestamp deleteTs        = new(0, 2500, 0);
        HLCTimestamp currentTime     = new(0, 3000, 0);

        MemoryPersistenceBackend memory = new();
        memory.StoreKeyValues([FlushedRow("doc/a", "a-disk")]);

        UnflushedKeyValueWritesIndex unflushed = new();
        unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v0"), 0, HLCTimestamp.Zero, firstRevisionTs, firstRevisionTs, KeyValueState.Set, false);
        unflushed.Record("doc/b", null, 1, HLCTimestamp.Zero, deleteTs, deleteTs, KeyValueState.Deleted, false);

        UnflushedOverlayPersistenceBackend overlay = new(memory, unflushed, new UnflushedLockWritesIndex());

        RangeDiskPage fenced = TryGetByRangeHandler.ReadDiskPage(
            overlay, "doc/", "doc/", cursorRowExcluded: false, endKey: null, endInclusive: false, limit: 10,
            isSnapshotRead: true, snapshotTs, currentTime, shouldAbort: null);

        Assert.Equal(["doc/b"], fenced.HistoryFencedKeys!.Keys.ToList());
        Assert.Equal(-1L, fenced.HistoryFencedKeys["doc/b"]);
        Assert.Equal(["doc/a"], fenced.Projected.Select(static i => i.Key).ToList());

        memory.StoreKeyValues(
        [
            new PersistenceRequestItem(
                key: "doc/b",
                value: Encoding.UTF8.GetBytes("b-v0"),
                revision: 0,
                expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                lastUsedNode: 0, lastUsedPhysical: 1500, lastUsedCounter: 0,
                lastModifiedNode: 0, lastModifiedPhysical: 1500, lastModifiedCounter: 0,
                state: (int)KeyValueState.Set)
        ]);
        unflushed.RemoveFlushed("doc/b", 0, firstRevisionTs);

        RangeDiskPage served = TryGetByRangeHandler.ReadDiskPage(
            overlay, "doc/", "doc/", cursorRowExcluded: false, endKey: null, endInclusive: false, limit: 10,
            isSnapshotRead: true, snapshotTs, currentTime, shouldAbort: null);

        Assert.Null(served.HistoryFencedKeys);
        Assert.Equal(["doc/a", "doc/b"], served.Projected.Select(static i => i.Key).ToList());
        Assert.Equal(0L, served.Projected[1].Entry.Revision);
        Assert.Equal(KeyValueState.Set, served.Projected[1].Entry.State);
    }

    /// <summary>
    /// The stage-2 projection of a row with flushed history below its queued revisions: the row is fenced, with the
    /// ceiling the history is complete below, and still projected from that history. Whether that projection is the
    /// row's as-of image depends on the queued revisions, which only the resident archive can see, so the decision is
    /// the merge's.
    /// </summary>
    [Fact]
    public void ProjectSnapshotPage_QueuedRevisionsAboveFlushedHistory_ProjectsTheRowBelowThemAndFencesIt()
    {
        HLCTimestamp flushedTs   = new(0, 1000, 0);
        HLCTimestamp snapshotTs  = new(0, 2000, 0);
        HLCTimestamp thirdTs     = new(0, 2400, 0);
        HLCTimestamp fourthTs    = new(0, 2500, 0);
        HLCTimestamp currentTime = new(0, 3000, 0);

        MemoryPersistenceBackend memory = new();
        memory.StoreKeyValues([FlushedRow("doc/a", "a-disk"), FlushedRow("doc/b", "b-v2", revision: 2, physical: flushedTs.L)]);

        // doc/b: revisions 3 and 4 committed after the snapshot, both still queued for the writer.
        UnflushedKeyValueWritesIndex unflushed = new();
        unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v3"), 3, HLCTimestamp.Zero, thirdTs, thirdTs, KeyValueState.Set, false);
        unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v4"), 4, HLCTimestamp.Zero, fourthTs, fourthTs, KeyValueState.Set, false);

        UnflushedOverlayPersistenceBackend overlay = new(memory, unflushed, new UnflushedLockWritesIndex());

        RangeDiskPage page = TryGetByRangeHandler.ReadDiskPage(
            overlay, "doc/", "doc/", cursorRowExcluded: false, endKey: null, endInclusive: false, limit: 10,
            isSnapshotRead: true, snapshotTs, currentTime, shouldAbort: null);

        Assert.Equal(["doc/b"], page.HistoryFencedKeys!.Keys.ToList());
        Assert.Equal(2L, page.HistoryFencedKeys["doc/b"]);
        Assert.Equal(["doc/a", "doc/b"], page.Projected.Select(static i => i.Key).ToList());
        Assert.Equal(2L, page.Projected[1].Entry.Revision);
        Assert.Equal("b-v2", Encoding.UTF8.GetString(page.Projected[1].Entry.Value!));
    }

    /// <summary>
    /// A resident row committed several times since the snapshot on a leader whose flusher is behind: every one of
    /// those revisions is still queued, so the projection fences the row, and the archive, trimmed to the unflushed
    /// revisions and the newest few, holds nothing at or before the snapshot. The row's as-of image is its last
    /// flushed revision. The projection takes it from the history below the queued revisions, and the merge serves it
    /// once the archive has shown every queued revision past the snapshot, instead of leaving the row out of the page
    /// (a read-committed COUNT(*) counted one row fewer than the table held).
    /// </summary>
    [Fact]
    public async Task SnapshotRangeScan_ResidentRowWithEveryQueuedRevisionPastTheSnapshot_ServesItsLastFlushedRevision()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-queued-history-flushed-projection");

        scheduler.Start();
        try
        {
            HLCTimestamp flushedTs  = new(0, 1000, 0);
            HLCTimestamp snapshotTs = new(0, 2000, 0);
            HLCTimestamp thirdTs    = new(0, 2400, 0);
            HLCTimestamp fourthTs   = new(0, 2500, 0);
            HLCTimestamp fifthTs    = new(0, 2600, 0);

            MemoryPersistenceBackend memory = new();
            memory.StoreKeyValues(
            [
                FlushedRow("doc/a", "a-disk"),
                FlushedRow("doc/b", "b-v2", revision: 2, physical: flushedTs.L),
                FlushedRow("doc/c", "c-disk")
            ]);

            UnflushedKeyValueWritesIndex unflushed = new();
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v3"), 3, HLCTimestamp.Zero, thirdTs, thirdTs, KeyValueState.Set, false);
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v4"), 4, HLCTimestamp.Zero, fourthTs, fourthTs, KeyValueState.Set, false);
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v5"), 5, HLCTimestamp.Zero, fifthTs, fifthTs, KeyValueState.Set, false);

            UnflushedOverlayPersistenceBackend overlay = new(memory, unflushed, new UnflushedLockWritesIndex());

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-queued-history-flushed-projection-actor", null!, null!, overlay, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            ((KeyValueActor)actorRef.Runner.Actor!).GetContext().InsertStoreEntry("doc/b", ResidentWithQueuedRevisions(thirdTs, fourthTs, fifthTs));

            KeyValueResponse? served = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, snapshotTs), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(served);
            Assert.Equal(KeyValueResponseType.Get, served!.Type);
            Assert.Equal(["doc/a", "doc/b", "doc/c"], served.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.Equal(2L, served.RangeResult.Items[1].Item2.Revision);
            Assert.Equal("b-v2", Encoding.UTF8.GetString(served.RangeResult.Items[1].Item2.Value!));
            Assert.False(served.RangeResult.HasMore);

            // A snapshot inside the queued revisions is served from the archive, as before.
            KeyValueResponse? archived = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, new HLCTimestamp(0, 2450, 0)), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(archived);
            Assert.Equal(KeyValueResponseType.Get, archived!.Type);
            Assert.Equal(["doc/a", "doc/b", "doc/c"], archived.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.Equal(3L, archived.RangeResult.Items[1].Item2.Revision);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// The projection of a fenced row is its as-of image only while the revisions it was taken below are the ones
    /// still queued. When the flush of one of them lands between the projection (stage 2) and the merge (stage 3),
    /// the archive may since have trimmed a revision the projection never saw, so the page answers MustRetry rather
    /// than serving the projection; the retry projects the row again, at the new ceiling, and serves it.
    /// </summary>
    [Fact]
    public async Task SnapshotRangeScan_FlushLandsBetweenProjectionAndMerge_RetriesInsteadOfServingTheProjection()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-queued-history-moved-ceiling");

        scheduler.Start();
        try
        {
            HLCTimestamp flushedTs  = new(0, 1000, 0);
            HLCTimestamp snapshotTs = new(0, 2000, 0);
            HLCTimestamp thirdTs    = new(0, 2400, 0);
            HLCTimestamp fourthTs   = new(0, 2500, 0);
            HLCTimestamp fifthTs    = new(0, 2600, 0);

            MemoryPersistenceBackend memory = new();
            memory.StoreKeyValues(
            [
                FlushedRow("doc/a", "a-disk"),
                FlushedRow("doc/b", "b-v2", revision: 2, physical: flushedTs.L)
            ]);

            UnflushedKeyValueWritesIndex unflushed = new();
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v3"), 3, HLCTimestamp.Zero, thirdTs, thirdTs, KeyValueState.Set, false);
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v4"), 4, HLCTimestamp.Zero, fourthTs, fourthTs, KeyValueState.Set, false);
            unflushed.Record("doc/b", Encoding.UTF8.GetBytes("b-v5"), 5, HLCTimestamp.Zero, fifthTs, fifthTs, KeyValueState.Set, false);

            // The flush of revision 3 lands right after the projection read doc/b's history below it.
            FlushLandingBackend landing = new(memory, "doc/b", () =>
            {
                memory.StoreKeyValues([FlushedRow("doc/b", "b-v3", revision: 3, physical: thirdTs.L)]);
                unflushed.RemoveFlushed("doc/b", 3, thirdTs);
            });

            UnflushedOverlayPersistenceBackend overlay = new(landing, unflushed, new UnflushedLockWritesIndex());

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-queued-history-moved-ceiling-actor", null!, null!, overlay, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            ((KeyValueActor)actorRef.Runner.Actor!).GetContext().InsertStoreEntry("doc/b", ResidentWithQueuedRevisions(thirdTs, fourthTs, fifthTs));

            KeyValueResponse? moved = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, snapshotTs), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(moved);
            Assert.Equal(KeyValueResponseType.MustRetry, moved!.Type);
            Assert.Equal(1, landing.Landed);

            KeyValueResponse? served = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, snapshotTs), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(served);
            Assert.Equal(KeyValueResponseType.Get, served!.Type);
            Assert.Equal(["doc/a", "doc/b"], served.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.Equal(2L, served.RangeResult.Items[1].Item2.Revision);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A resident entry as the archive trim leaves it on a leader whose flusher is behind: the head and the queued
    /// revisions below it (3 and 4) are retained, the flushed revision 2 below them was trimmed.
    /// </summary>
    private static KeyValueEntry ResidentWithQueuedRevisions(HLCTimestamp thirdTs, HLCTimestamp fourthTs, HLCTimestamp fifthTs)
    {
        KeyValueEntry resident = new()
        {
            Bucket = "doc",
            Value = Encoding.UTF8.GetBytes("b-v5"),
            Revision = 5,
            FlushedRevision = 2,
            LastModified = fifthTs,
            LastUsed = fifthTs,
            State = KeyValueState.Set,
            Revisions = new()
        };
        resident.Revisions[3] = new KeyValueRevisionEntry(Encoding.UTF8.GetBytes("b-v3"), thirdTs, HLCTimestamp.Zero, KeyValueState.Set);
        resident.Revisions[4] = new KeyValueRevisionEntry(Encoding.UTF8.GetBytes("b-v4"), fourthTs, HLCTimestamp.Zero, KeyValueState.Set);
        return resident;
    }

    /// <summary>
    /// A backend that runs <paramref name="onHistoryRead"/> once, right after the first as-of history read of
    /// <paramref name="key"/>: a flush that lands between a scan's projection and its merge.
    /// </summary>
    private sealed class FlushLandingBackend(MemoryPersistenceBackend inner, string key, Action onHistoryRead) : IPersistenceBackend
    {
        private int landed;

        internal int Landed => landed;

        public bool ServesReadsFromMemory => inner.ServesReadsFromMemory;
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);

        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
        {
            ReadOnlyKeyValueEntry? row = inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);

            if (keyName == key && Interlocked.Exchange(ref landed, 1) == 0)
                onHistoryRead();

            return row;
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

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
            0, 1, default);
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
            0, 0, default);

    // ── Deferred settlement landing between the pages of one scan ─────────────────────────

    /// <summary>
    /// A committed-but-unsettled move (one row deleted, another inserted) settles while a snapshot scan is between
    /// two of its disk pages. The page holding the insert's position was read before the settlement, when the
    /// intent was the only copy of the insert; the page holding the delete's position after it, from a base that
    /// already carries the tombstone. The overlay must still inject the insert and not resurrect the delete. On a
    /// newly promoted leader, where no write intent makes the page wait for the predecessor's late-settling
    /// commits, a scan overlaid with the live intent window read after the settlement counted 499 of 500 rows a
    /// second after a leader-balancer handover: the inserted row id sorted before the deleted one.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SnapshotRangeScan_SettlementLandsBetweenPages_InsertSortsFirst_StillServesTheCommittedMove(bool servesReadsFromMemory)
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-settle-between-pages-insert-first");

        scheduler.Start();
        try
        {
            HLCTimestamp txId       = new(0, 100, 0);
            HLCTimestamp commitTs   = new(0, 1500, 0);
            HLCTimestamp snapshotTs = new(0, 2000, 0);

            // A page holds limit + 1 = 6 raw rows: the first is doc/a and five tombstones, so doc/x needs a second
            // page. doc/b (the insert) has its position on the first page and is absent from the base until the
            // settlement; doc/x (the delete) is live in the base until then.
            MemoryPersistenceBackend memory = new();
            memory.StoreKeyValues(
            [
                FlushedRow("doc/a", "a-v1"),
                TombstoneRow("doc/c"), TombstoneRow("doc/d"), TombstoneRow("doc/e"), TombstoneRow("doc/f"), TombstoneRow("doc/g"),
                FlushedRow("doc/x", "x-v1")
            ]);

            PreparedIntentStore intents = new();
            InjectCommittedIntent(intents, txId, "doc/b", "b-v1", revision: 1, commitTs);
            InjectCommittedIntent(intents, txId, "doc/x", null, revision: 2, commitTs);

            // The settlement lands as the second page is read: the value and the tombstone reach the base and
            // the intents are removed.
            SettlementLandingBackend backend = new(memory, pageIndex: 1, servesReadsFromMemory, () =>
            {
                memory.StoreKeyValues(
                [
                    FlushedRow("doc/b", "b-v1", revision: 1, physical: commitTs.L),
                    TombstoneRow("doc/x", revision: 2, physical: commitTs.L)
                ]);
                intents.Apply(new RemoveIntentCommand(txId, 1, "doc/b"));
                intents.Apply(new RemoveIntentCommand(txId, 1, "doc/x"));
            });

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                SpawnWithIntents(actorSystem, "range-settle-between-pages-insert-first-actor", backend, raft, config, logger, intents);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 5, snapshotTs), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(1, backend.Landed);
            Assert.Equal(["doc/a", "doc/b"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
            Assert.Equal("b-v1", Encoding.UTF8.GetString(resp.RangeResult.Items[1].Item2.Value!));
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// The same settlement with the delete's position on the page read before it and the insert's on the page
    /// read after it: the deleted row was read live from the base, and the overlay must still remove it (the
    /// mirror image of the short count — one row too many).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SnapshotRangeScan_SettlementLandsBetweenPages_DeleteSortsFirst_DoesNotResurrectTheDeletedRow(bool servesReadsFromMemory)
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-settle-between-pages-delete-first");

        scheduler.Start();
        try
        {
            HLCTimestamp txId       = new(0, 100, 0);
            HLCTimestamp commitTs   = new(0, 1500, 0);
            HLCTimestamp snapshotTs = new(0, 2000, 0);

            MemoryPersistenceBackend memory = new();
            memory.StoreKeyValues(
            [
                FlushedRow("doc/a", "a-v1"),
                FlushedRow("doc/b", "b-v1"),
                TombstoneRow("doc/c"), TombstoneRow("doc/d"), TombstoneRow("doc/e"), TombstoneRow("doc/f"), TombstoneRow("doc/g")
            ]);

            PreparedIntentStore intents = new();
            InjectCommittedIntent(intents, txId, "doc/b", null, revision: 2, commitTs);
            InjectCommittedIntent(intents, txId, "doc/y", "y-v1", revision: 1, commitTs);

            SettlementLandingBackend backend = new(memory, pageIndex: 1, servesReadsFromMemory, () =>
            {
                memory.StoreKeyValues(
                [
                    TombstoneRow("doc/b", revision: 2, physical: commitTs.L),
                    FlushedRow("doc/y", "y-v1", revision: 1, physical: commitTs.L)
                ]);
                intents.Apply(new RemoveIntentCommand(txId, 1, "doc/b"));
                intents.Apply(new RemoveIntentCommand(txId, 1, "doc/y"));
            });

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                SpawnWithIntents(actorSystem, "range-settle-between-pages-delete-first-actor", backend, raft, config, logger, intents);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 5, snapshotTs), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(1, backend.Landed);
            Assert.Equal(["doc/a", "doc/y"], resp.RangeResult!.Items.Select(static i => i.Item1).ToList());
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A snapshot scan keeps a committed intent under a head committed after the snapshot. The page's row for the
    /// key comes from the history below that head, and when that history lacks the intent's revision the row is
    /// older than the intent, which must override it. The head alone used to supersede the intent, and the older
    /// row stood.
    /// </summary>
    [Fact]
    public async Task SnapshotRangeScan_CommittedIntentUnderAHeadPastTheSnapshot_OverridesTheOlderProjection()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-intent-under-later-head");

        scheduler.Start();
        try
        {
            HLCTimestamp txId       = new(0, 100, 0);
            HLCTimestamp firstTs    = new(0, 1000, 0);
            HLCTimestamp commitTs   = new(0, 1500, 0);
            HLCTimestamp snapshotTs = new(0, 2000, 0);
            HLCTimestamp laterTs    = new(0, 2500, 0);

            // The persisted history holds revisions 1 and 3; revision 2, the intent's, never reached it.
            MemoryPersistenceBackend memory = new();
            memory.StoreKeyValues([FlushedRow("doc/k", "k-v1", revision: 1, physical: firstTs.L)]);
            memory.StoreKeyValues([FlushedRow("doc/k", "k-v3", revision: 3, physical: laterTs.L)]);

            PreparedIntentStore intents = new();
            InjectCommittedIntent(intents, txId, "doc/k", "k-v2", revision: 2, commitTs);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                SpawnWithIntents(actorSystem, "range-intent-under-later-head-actor", memory, raft, config, logger, intents);

            // Resident at the later head, with no archive: the as-of read falls back to the disk projection.
            ((KeyValueActor)actorRef.Runner.Actor!).GetContext().InsertStoreEntry("doc/k", new KeyValueEntry
            {
                Bucket = "doc",
                Value = Encoding.UTF8.GetBytes("k-v3"),
                Revision = 3,
                FlushedRevision = 3,
                LastUsed = laterTs,
                LastModified = laterTs,
                State = KeyValueState.Set
            });

            KeyValueResponse? resp = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, snapshotTs), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            (string key, ReadOnlyKeyValueEntry row) = Assert.Single(resp.RangeResult!.Items);
            Assert.Equal("doc/k", key);
            Assert.Equal(2L, row.Revision);
            Assert.Equal("k-v2", Encoding.UTF8.GetString(row.Value!));
        }
        finally { scheduler.Stop(); }
    }

    private static PersistenceRequestItem TombstoneRow(string key, long revision = 1, long physical = 1000) =>
        new(
            key: key,
            value: null,
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: physical, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: physical, lastModifiedCounter: 0,
            state: (int)KeyValueState.Deleted);

    /// <summary>
    /// Installs a prepared intent of <paramref name="transactionId"/> on <paramref name="key"/> and resolves it
    /// committed without settling it: the window between the durable decision and the settlement. A null value is
    /// a committed delete.
    /// </summary>
    private static void InjectCommittedIntent(
        PreparedIntentStore store, HLCTimestamp transactionId, string key, string? value, long revision, HLCTimestamp commitTs)
    {
        store.Apply(new PrepareIntentCommand(new PreparedIntent(
            TransactionId: transactionId, Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
            CommitTimestamp: commitTs,
            State: value is null ? KeyValueState.Deleted : KeyValueState.Set,
            Value: value is null ? null : Encoding.UTF8.GetBytes(value),
            Bucket: "doc", Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending)));
        store.Apply(new ResolveIntentCommand(transactionId, 1, key, Commit: true));
    }

    private static IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> SpawnWithIntents(
        ActorSystem actorSystem, string name, IPersistenceBackend backend, RaftManager raft,
        KahunaConfiguration config, ILogger<IKahuna> logger, PreparedIntentStore intents) =>
        actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
            name, null!, null!, backend, raft, raft.ReadScheduler, new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger), config, logger,
            null!, null!, intents, null!);

    /// <summary>
    /// A backend that runs <paramref name="onPageRead"/> once, as the page read numbered <paramref name="pageIndex"/>
    /// (counted from zero) is issued and before it is served: a settlement that lands between two pages of one scan.
    /// </summary>
    private sealed class SettlementLandingBackend(
        MemoryPersistenceBackend inner, int pageIndex, bool servesReadsFromMemory, Action onPageRead) : IPersistenceBackend
    {
        private int pagesRead;
        private int landed;

        internal int Landed => landed;

        public bool ServesReadsFromMemory => servesReadsFromMemory;
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
        {
            if (Interlocked.Increment(ref pagesRead) - 1 == pageIndex && Interlocked.Exchange(ref landed, 1) == 0)
                onPageRead();

            return inner.GetKeyValueByRange(prefix, startKey, limit);
        }

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    // ── Raft / config factory ─────────────────────────────────────────────────────────────

    private (RaftManager, FairReadScheduler, KahunaConfiguration, ILogger<IKahuna>) CreateRaftAndConfig(string nodeName)
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
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        return (raft, (FairReadScheduler)Track(raft).ReadScheduler, config, logger);
    }

    // ── Inner backends ────────────────────────────────────────────────────────────────────

    /// <summary>One backend page read as the handler issued it, and how many entries it got back.</summary>
    private readonly record struct PageRead(string? StartKey, string? EndKey, bool EndInclusive, int Limit, int Returned);

    /// <summary>
    /// Backend with a flat sorted list of entries. GetKeyValueByRange returns up to `limit`
    /// items with keys that start with `prefix` and are &gt;= `startKey` (if provided), and stops at
    /// the end bound when one is given. Every page read is recorded so a test can assert how many
    /// entries the handler asked the backend for.
    /// </summary>
    private sealed class RangeBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> allEntries;
        private readonly List<PageRead> reads = [];

        /// <summary>The page reads so far, oldest first. Read it after the scan's response arrived.</summary>
        internal List<PageRead> Reads
        {
            get { lock (reads) return [.. reads]; }
        }

        /// <summary>
        /// Whether the handler may read pages on the actor thread. False (the interface default) keeps every page
        /// on the scheduler path; true takes the path of the in-memory backend, over the same recorded reads.
        /// </summary>
        public bool ServesReadsFromMemory { get; }

        internal RangeBackend(IEnumerable<string> keys, Func<string, KeyValueState>? stateOf = null, bool servesReadsFromMemory = false)
        {
            ServesReadsFromMemory = servesReadsFromMemory;
            allEntries = keys
                .OrderBy(k => k, StringComparer.Ordinal)
                .Select(k => (k, new ReadOnlyKeyValueEntry(
                    Encoding.UTF8.GetBytes(k + "-val"), 1L,
                    HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, stateOf?.Invoke(k) ?? KeyValueState.Set)))
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
        public LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
        {
            KeyValueScanTally discarded = default;
            return GetKeyValueByRange(prefix, startKey, null, false, limit, ref discarded);
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(
            string prefix, string? startKey, string? endKey, bool endInclusive, int limit, ref KeyValueScanTally tally)
        {
            IEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> candidates =
                allEntries.Where(e => e.Key.StartsWith(prefix, StringComparison.Ordinal));

            if (startKey is not null)
                candidates = candidates.Where(e => string.CompareOrdinal(e.Key, startKey) >= 0);

            if (endKey is not null)
                candidates = candidates.Where(e => IPersistenceBackend.IsWithinEndBound(e.Key, endKey, endInclusive));

            List<(string, ReadOnlyKeyValueEntry)> page = candidates
                .Take(limit)
                .Select(e => (e.Key, e.Entry))
                .ToList();

            lock (reads)
                reads.Add(new PageRead(startKey, endKey, endInclusive, limit, page.Count));

            return page;
        }

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    // ── Snapshot disk projection unit tests ───────────────────────────────────────────────

    /// <summary>
    /// Disk-only key whose current revision was committed after the snapshot timestamp. The
    /// stage-2 ProjectSnapshotPage lambda replaces it with the revision at-or-before the
    /// snapshot (returned by GetKeyValueRevisionAtOrBefore). Stage-3 EvaluateKeySync sees an
    /// entry whose LastModified is already &lt;= snapshotTs and returns it via the non-snapshot
    /// path — the disk fallback carries the as-of value into the result.
    /// </summary>
    [Fact]
    public async Task SnapshotRangeScan_DiskOnlyKey_ProjectedToAsOfRevision()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("snap-disk-proj");

        scheduler.Start();
        try
        {
            HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            HLCTimestamp afterTs    = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // Backend: "snap/a" exists only on disk. Current revision is newer than snapshotTs;
            // GetKeyValueRevisionAtOrBefore returns the as-of entry.
            SnapshotProjectionBackend backend = new(
                currentKey:          "snap/a",
                currentRevision:     5,
                currentLastModified: afterTs,
                currentValue:        Encoding.UTF8.GetBytes("v-current"),
                asOfRevision:        3,
                asOfLastModified:    snapshotTs,
                asOfValue:           Encoding.UTF8.GetBytes("v-at-snapshot"));

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "snap-disk-proj-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueRequest scan = MakeSnapshotRangeScan("snap/", limit: 10, snapshotTs);
            KeyValueResponse? resp = await actorRef.Ask(scan, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Single(resp.RangeResult!.Items);
            Assert.Equal("snap/a", resp.RangeResult.Items[0].Item1);
            Assert.Equal("v-at-snapshot", Encoding.UTF8.GetString(resp.RangeResult.Items[0].Item2.Value!));
            Assert.Equal(3L, resp.RangeResult.Items[0].Item2.Revision);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// Disk-only key deleted as of the snapshot timestamp. The stage-2 projection resolves
    /// to State=Deleted and the key is excluded from the scan result.
    /// </summary>
    [Fact]
    public async Task SnapshotRangeScan_DiskOnlyKey_DeletedAtSnapshot_Excluded()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("snap-disk-del");

        scheduler.Start();
        try
        {
            HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            HLCTimestamp afterTs    = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // "snap/b" has a current (non-deleted) state newer than snapshotTs.
            // As of snapshotTs the key was deleted — GetKeyValueRevisionAtOrBefore returns Deleted.
            SnapshotProjectionBackend backend = new(
                currentKey:          "snap/b",
                currentRevision:     8,
                currentLastModified: afterTs,
                currentValue:        Encoding.UTF8.GetBytes("v-restored"),
                asOfRevision:        4,
                asOfLastModified:    snapshotTs,
                asOfValue:           null,
                asOfState:           KeyValueState.Deleted);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "snap-disk-del-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueRequest scan = MakeSnapshotRangeScan("snap/", limit: 10, snapshotTs);
            KeyValueResponse? resp = await actorRef.Ask(scan, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Empty(resp.RangeResult!.Items);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// The first raw disk page contains exactly limit+1 entries that are all post-snapshot
    /// (dropped by projection). The scan must dispatch a second page and return the key that
    /// was visible at the snapshot timestamp but sorts after all the post-snapshot keys.
    /// Both page paths: read on the scheduler, and read on the actor thread as for an in-memory backend.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SnapshotRangeScan_FirstPageAllPostSnapshot_VisibleKeyOnLaterPage_Returned(bool servesReadsFromMemory)
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("snap-pagination-" + servesReadsFromMemory);

        scheduler.Start();
        try
        {
            HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            HLCTimestamp afterTs    = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // snap/001..snap/011 — current revision committed after snapshot; no history.
            // snap/012 — LastModified == snapshotTs; visible at the snapshot.
            PostSnapshotPaginationBackend backend = new(snapshotTs, afterTs, servesReadsFromMemory);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "snap-pagination-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // limit=10: first raw page has 11 entries (limit+1), all post-snapshot and projected
            // away. Without the fix diskHasMore=false (derived from projected.Count) and the scan
            // returns nothing. With the fix RawHasMore=true and a second page is dispatched.
            KeyValueRequest scan = MakeSnapshotRangeScan("snap/", limit: 10, snapshotTs);
            KeyValueResponse? resp = await actorRef.Ask(scan, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Single(resp.RangeResult!.Items);
            Assert.Equal("snap/012", resp.RangeResult.Items[0].Item1);
            Assert.Equal("v-visible", System.Text.Encoding.UTF8.GetString(resp.RangeResult.Items[0].Item2.Value!));
        }
        finally { scheduler.Stop(); }
    }

    private static KeyValueRequest MakeSnapshotRangeScan(string prefix, int limit, HLCTimestamp snapshotTs)
    {
        KeyValueRequest req = new(
            KeyValueRequestType.GetByRange,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            prefix,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0, 1, default);
        req.Limit = limit;
        req.StartInclusive = true;
        req.ReadTimestamp = snapshotTs;
        return req;
    }

    // ── Same-key memory/disk collisions: decided by (revision, commit HLC), not residency ──

    private static KeyValueRequest MakeGet(string key) =>
        new(
            KeyValueRequestType.TryGet,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key,
            null,
            null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            0, 0, default);

    /// <summary>
    /// A stale resident entry that missed its coherence notification must not shadow a newer
    /// durable row: the plain current-read scan serves the newer disk row, and the collision
    /// schedules a coherence reconcile that converges the resident entry off the scan.
    /// </summary>
    [Fact]
    public async Task PersistentRangeScan_SameKeyCollision_NewerDiskRowWinsAndHealsTheResident()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-collision-disk-newer");

        scheduler.Start();
        try
        {
            // Disk holds doc/x at revision 5; the resident entry is behind it.
            RangeBackend backend = new(new Dictionary<string, (byte[]? Value, long Revision)>
            {
                ["doc/x"] = (Encoding.UTF8.GetBytes("disk-v5"), 5L)
            });

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-collision-disk-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            await actorRef.Ask(
                MakeSet("doc/x", Encoding.UTF8.GetBytes("mem-stale")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Single(resp.RangeResult!.Items);
            Assert.Equal("doc/x", resp.RangeResult.Items[0].Item1);
            Assert.Equal(5, resp.RangeResult.Items[0].Item2.Revision);
            Assert.Equal("disk-v5", Encoding.UTF8.GetString(resp.RangeResult.Items[0].Item2.Value!));

            // The collision also scheduled a reconcile: the resident entry converges to the
            // durable row without any further scan touching disk.
            long deadline = Environment.TickCount64 + 5_000;
            while (true)
            {
                KeyValueResponse? read = await actorRef.Ask(
                    MakeGet("doc/x"), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

                if (read?.Entry?.Revision == 5)
                {
                    Assert.Equal("disk-v5", Encoding.UTF8.GetString(read.Entry.Value!));
                    break;
                }

                Assert.True(Environment.TickCount64 < deadline, "the resident entry did not converge to the newer durable row");
                await Task.Delay(20, TestContext.Current.CancellationToken);
            }
        }
        finally { scheduler.Stop(); }
    }

    [Fact]
    public async Task PersistentRangeScan_SameKeyCollision_NewerResidentStillWins()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-collision-mem-newer");

        scheduler.Start();
        try
        {
            // Disk holds doc/y at revision 0; the resident entry advances past it with fresh sets.
            RangeBackend backend = new(new Dictionary<string, (byte[]? Value, long Revision)>
            {
                ["doc/y"] = (Encoding.UTF8.GetBytes("disk-old"), 0L)
            });

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-collision-mem-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            await actorRef.Ask(
                MakeSet("doc/y", Encoding.UTF8.GetBytes("mem-1")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await actorRef.Ask(
                MakeSet("doc/y", Encoding.UTF8.GetBytes("mem-2")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeRangeScan("doc/", limit: 10), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Single(resp.RangeResult!.Items);
            Assert.Equal("mem-2", Encoding.UTF8.GetString(resp.RangeResult.Items[0].Item2.Value!));
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// Snapshot scans keep their as-of projection rules: a newer disk row on a same-key collision
    /// neither replaces the served value nor schedules a heal from the snapshot path — the
    /// current-read branch owns convergence.
    /// </summary>
    [Fact]
    public async Task SnapshotRangeScan_SameKeyCollision_KeepsAsOfServingAndDoesNotHeal()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-collision-snapshot");

        scheduler.Start();
        try
        {
            RangeBackend backend = new(new Dictionary<string, (byte[]? Value, long Revision)>
            {
                ["doc/z"] = (Encoding.UTF8.GetBytes("disk-v5"), 5L)
            });

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-collision-snap-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? written = await actorRef.Ask(
                MakeSet("doc/z", Encoding.UTF8.GetBytes("mem-as-of")),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.NotNull(written);

            HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            KeyValueResponse? resp = await actorRef.Ask(
                MakeSnapshotRangeScan("doc/", limit: 10, snapshotTs),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Single(resp.RangeResult!.Items);
            Assert.Equal("mem-as-of", Encoding.UTF8.GetString(resp.RangeResult.Items[0].Item2.Value!));

            // No heal may originate from the snapshot path: the resident entry stays as written.
            await Task.Delay(200, TestContext.Current.CancellationToken);
            KeyValueResponse? read = await actorRef.Ask(
                MakeGet("doc/z"), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal("mem-as-of", Encoding.UTF8.GetString(read!.Entry!.Value!));
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// Backend that signals `pageEntered` when the first GetKeyValueByRange call arrives,
    /// then blocks on `gate` before returning — allowing a concurrent write to be processed
    /// by the actor while stage 2 is in flight.
    /// </summary>
    private sealed class BlockingRangeBackend : IPersistenceBackend, IDisposable
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
        public LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
        {
            int n = Interlocked.Increment(ref callCount);

            // Only block on the first page read — that is the point where stage 2 is in
            // flight and the test fires the concurrent write.
            if (n == 1)
            {
                pageEntered.Set();
                gate.Wait(TestContext.Current.CancellationToken);
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

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// Backend with a single key that has a current revision newer than a snapshot timestamp,
    /// and a historical revision (returned by GetKeyValueRevisionAtOrBefore) matching the snapshot.
    /// Used to verify that the stage-2 ProjectSnapshotPage lambda replaces the current entry with
    /// the as-of entry, and that stage-3 EvaluateKeySync serves it correctly.
    /// </summary>
    private sealed class SnapshotProjectionBackend : IPersistenceBackend, IDisposable
    {
        private readonly string key;
        private readonly ReadOnlyKeyValueEntry currentEntry;
        private readonly KeyValueEntry asOfEntry;

        internal SnapshotProjectionBackend(
            string currentKey, long currentRevision, HLCTimestamp currentLastModified, byte[]? currentValue,
            long asOfRevision, HLCTimestamp asOfLastModified, byte[]? asOfValue,
            KeyValueState asOfState = KeyValueState.Set)
        {
            key = currentKey;
            currentEntry = new(currentValue, currentRevision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, currentLastModified, KeyValueState.Set);
            asOfEntry = new KeyValueEntry
            {
                Value = asOfValue,
                Revision = asOfRevision,
                LastModified = asOfLastModified,
                State = asOfState
            };
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal)) return [];
            if (startKey is not null && string.CompareOrdinal(key, startKey) < 0) return [];
            return [(key, currentEntry)];
        }

        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
        {
            if (keyName != key) return null;
            return asOfEntry.LastModified.CompareTo(readTimestamp) <= 0 ? asOfEntry.ToRow() : null;
        }

        // Remaining methods delegate to an empty in-memory backend.
        private readonly MemoryPersistenceBackend inner = new();
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockRow? GetLock(string resource) => null;
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => null;
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => null;
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => [];
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// Backend with 12 keys sorted as snap/001..snap/012. The first 11 have current revisions
    /// committed after the snapshot timestamp (no historical as-of revision), so they are
    /// dropped by ProjectSnapshotPage projection. snap/012 has LastModified == snapshotTs and
    /// is visible at the snapshot. Used to verify that a first raw page of limit+1 post-snapshot
    /// keys does not prematurely terminate the scan (R1 defect: pagination must be derived from
    /// the raw page, not the projected page).
    /// </summary>
    private sealed class PostSnapshotPaginationBackend : IPersistenceBackend, IDisposable
    {
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> allEntries;

        public bool ServesReadsFromMemory { get; }

        internal PostSnapshotPaginationBackend(HLCTimestamp snapshotTs, HLCTimestamp afterTs, bool servesReadsFromMemory = false)
        {
            ServesReadsFromMemory = servesReadsFromMemory;
            List<(string, ReadOnlyKeyValueEntry)> entries = new();
            for (int i = 1; i <= 11; i++)
            {
                string k = $"snap/{i:D3}";
                entries.Add((k, new ReadOnlyKeyValueEntry(
                    Encoding.UTF8.GetBytes(k + "-after"), 2L,
                    HLCTimestamp.Zero, HLCTimestamp.Zero, afterTs, KeyValueState.Set)));
            }
            entries.Add(("snap/012", new ReadOnlyKeyValueEntry(
                Encoding.UTF8.GetBytes("v-visible"), 1L,
                HLCTimestamp.Zero, HLCTimestamp.Zero, snapshotTs, KeyValueState.Set)));
            allEntries = entries;
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
        {
            IEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> candidates =
                allEntries.Where(e => e.Key.StartsWith(prefix, StringComparison.Ordinal));
            if (startKey is not null)
                candidates = candidates.Where(e => string.CompareOrdinal(e.Key, startKey) >= 0);
            return candidates.Take(limit).Select(e => (e.Key, e.Entry)).ToList();
        }

        // snap/001..snap/011 have no snapshot history — return null (projected away).
        // snap/012 is served via the fast path (LastModified <= snapshotTs) and never hits this.
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => null;

        private readonly MemoryPersistenceBackend inner = new();
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockRow? GetLock(string resource) => null;
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => null;
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => null;
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => [];
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    // ── Non-snapshot scan must never drop a resident key with a live committed head ───────

    /// <summary>
    /// A read-committed scan carries no read timestamp, yet stage 1 stamps "now" on the continuation
    /// so the cursor can carry it. A resident key written after that stamp used to be evaluated as a
    /// snapshot read: once its in-memory archive held no revision at-or-before the stamp — a hot key
    /// trims the archive to RevisionRetention, a head jump leaves a gap — the lookup missed, a
    /// non-snapshot page has no disk projection to fall back on, and the key was silently dropped
    /// although it has a live committed head. Under the bank load a read_committed COUNT(*) over 2,000
    /// never-deleted rows returned 1,962-1,999 (CamusDB feature e31cf9bc). A non-snapshot page must
    /// serve the committed head, exactly as a point read without a timestamp does.
    /// </summary>
    [Fact]
    public async Task PersistentRangeScan_ResidentKeyOverwrittenPastItsArchiveDuringTheScan_IsStillReturned()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("range-hot-key-kept");

        scheduler.Start();
        try
        {
            ManualResetEventSlim gate = new(false);
            ManualResetEventSlim pageEntered = new(false);

            // One disk-only key so the scan must detach for a disk page; the resident key is the one
            // under test. The first disk read blocks on 'gate' after signalling 'pageEntered'.
            BlockingRangeBackend backend = new(gate, pageEntered, ["rng/z"]);

            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "range-hot-key-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Resident before the scan starts, so the stage-1 memory snapshot captures it.
            KeyValueResponse? seeded = await actorRef.Ask(
                MakeSet("rng/a", Encoding.UTF8.GetBytes("v0")), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, seeded!.Type);

            Task<KeyValueResponse?> scanTask = actorRef.Ask(
                MakeRangeScan("rng/", limit: 10), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            Assert.True(pageEntered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken),
                "Stage-2 disk read should have started within 5 s");

            // While the disk page is in flight, overwrite the resident key more times than the archive
            // retains (RevisionRetention = 16): every archived revision now post-dates the scan's stamp,
            // so an as-of lookup at the stamp misses. The key still has a live committed head.
            for (int i = 1; i <= 20; i++)
            {
                KeyValueResponse? w = await actorRef.Ask(
                    MakeSet("rng/a", Encoding.UTF8.GetBytes($"v{i}")), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, w!.Type);
            }

            gate.Set();

            KeyValueResponse? resp = await scanTask;

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.RangeResult);
            Assert.Equal(2, resp.RangeResult!.Items.Count);
            Assert.Equal("rng/a", resp.RangeResult.Items[0].Item1);
            Assert.Equal("v20", Encoding.UTF8.GetString(resp.RangeResult.Items[0].Item2.Value!));
            Assert.Equal("rng/z", resp.RangeResult.Items[1].Item1);
            Assert.False(resp.RangeResult.HasMore);
        }
        finally { scheduler.Stop(); }
    }
}

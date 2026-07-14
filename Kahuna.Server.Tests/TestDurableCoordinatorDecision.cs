
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
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
/// Acceptance tests for the durable coordinator decision integrated into two-phase commit: the anchored record
/// installed atomically with the anchor commit, ack ordering, completion, the durable-policy prepare guards,
/// and capacity rejection.
///
/// <para>The full request-path lifecycle (progress acks → <c>Completed</c>) only advances when the coordinator
/// session runs on the node that leads the anchor's data partition — a follower cannot replicate the progress
/// delta, and driving that to a remote anchor leader is the per-partition-leader recovery driver's job. The
/// coordinator session is routed by its coordinator key, which need not share a partition with the anchor, so
/// the lifecycle tests run on a single embedded node (one leader for every partition) to make colocation
/// deterministic. A separate multi-node test asserts the initial record replicates to every node from the one
/// committed anchor proposal.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableCoordinatorDecision
{
    private static EmbeddedKahunaOptions EmbeddedOptions(
        int outcomeRetentionMax = 10_000, int durableOutstandingMax = 100_000) => new()
    {
        Storage                        = "memory",
        WalStorage                     = "memory",
        InitialPartitions              = 1,
        TransactionOutcomeRetentionMax = outcomeRetentionMax,
        DurableDecisionOutstandingMax  = durableOutstandingMax,
    };

    private static CoordinatorDecisionStore Store(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).CoordinatorDecisionStore;

    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> RetrySet(
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (true)
        {
            (KeyValueResponseType type, long rev, HLCTimestamp ts) = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, rev, ts);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<CoordinatorDecisionRecord?> WaitForDecision(
        EmbeddedKahunaNode node, HLCTimestamp txId, CoordinatorDecisionStatus? wantStatus, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (true)
        {
            if (Store(node).TryGet(txId, out CoordinatorDecisionRecord rec) &&
                (wantStatus is null || rec.Status == wantStatus))
                return rec;

            if (Environment.TickCount64 >= deadline)
                return null;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<(KeyValueResponseType, TransactionHandle)> StartDurable(EmbeddedKahunaNode node, string coordinatorKey)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey     = coordinatorKey,
                Locking            = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                AsyncRelease       = false,
                Timeout            = 10_000,
            },
            ct);
    }

    private static async Task WriteInTxn(EmbeddedKahunaNode node, TransactionHandle handle, string key, byte[] value, KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType type, _, _) = await RetrySet(() =>
            node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, key, value, null, -1,
                KeyValueFlags.None, 0, durability, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task AssertValueCommitted(EmbeddedKahunaNode node, string key, byte[] expected)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType r, ReadOnlyKeyValueEntry? e) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, r);
        Assert.Equal(expected, e!.Value);
    }

    // Registers a non-modified point lock on the transaction's coordinator working set.
    private static async Task AcquireLockInTxn(EmbeddedKahunaNode node, TransactionHandle handle, string key, KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType type, _, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            handle.TransactionId, key, 30_000, durability, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, type);
    }

    // Registers a held prefix lock on the transaction's coordinator working set.
    private static async Task AcquirePrefixLockInTxn(EmbeddedKahunaNode node, TransactionHandle handle, string prefix, KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        KeyValueResponseType type = await node.Kahuna.LocateAndTryAcquireExclusivePrefixLock(
            handle.TransactionId, prefix, 30_000, durability, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, type);
    }

    // Registers a held range lock on the transaction's coordinator working set.
    private static async Task AcquireRangeLockInTxn(
        EmbeddedKahunaNode node, TransactionHandle handle, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndTryAcquireRangeLock(
            handle.TransactionId, prefix, startKey, startInclusive, endKey, endInclusive, 30_000, durability,
            RangeLockMode.Exclusive, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, type);
    }

    // Records a non-modified tracked read on the transaction's coordinator working set.
    private static async Task ReadInTxn(EmbeddedKahunaNode node, TransactionHandle handle, string key, KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, durability, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.True(type is KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist);
    }

    // Probes whether an exclusive lock on the key is currently held by another transaction: a fresh transaction
    // that cannot acquire it (AlreadyLocked) means it is held; Locked means it is free.
    private static async Task<KeyValueResponseType> ProbeExclusiveLock(EmbeddedKahunaNode node, string probeCoordinatorKey, string key, KeyValueDurability durability)
    {
        (KeyValueResponseType _, TransactionHandle probe) = await StartDurable(node, probeCoordinatorKey);
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType type, _, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            probe.TransactionId, key, 5_000, durability, ct);
        return type;
    }

    // ── A durable multi-key commit installs a Completed record anchored on the data partition ──────────────

    [Fact]
    public async Task DurableCommit_MultiKey_InstallsCompletedDecisionAndCommitsValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey = "durable:aaa";
        const string key2      = "durable:bbb";
        const string key3      = "durable:ccc";
        byte[] v1 = "one"u8.ToArray();
        byte[] v2 = "two"u8.ToArray();
        byte[] v3 = "three"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-1");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        // The anchor is the first confirmed persistent modified key: write it first.
        await WriteInTxn(node, handle, anchorKey, v1, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, key2, v2, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, key3, v3, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, string? committedAnchor) =
            await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitResult);
        Assert.Equal(anchorKey, committedAnchor);

        await AssertValueCommitted(node, anchorKey, v1);
        await AssertValueCommitted(node, key2, v2);
        await AssertValueCommitted(node, key3, v3);

        // The decision record reached Completed with every participant acknowledged, anchored on the first
        // modified key.
        CoordinatorDecisionRecord? record = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(record);
        Assert.Equal(anchorKey, record!.RecordAnchorKey);
        Assert.Equal(3, record.Participants.Count);
        Assert.True(record.AllParticipantsAcked);
        Assert.Contains(record.Participants, p => p.Key == anchorKey && p.Acked);
        Assert.Contains(record.Participants, p => p.Key == key2 && p.Acked);
        Assert.Contains(record.Participants, p => p.Key == key3 && p.Acked);
    }

    // ── A single-key durable commit records only the anchor participant, already acknowledged ──────────────

    [Fact]
    public async Task DurableCommit_SingleKey_RecordHasAnchorParticipantAcked()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey = "durable-single:key";
        byte[] value = "solo"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-single");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        await WriteInTxn(node, handle, anchorKey, value, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, string? committedAnchor) =
            await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitResult);
        Assert.Equal(anchorKey, committedAnchor);

        await AssertValueCommitted(node, anchorKey, value);

        CoordinatorDecisionRecord? record = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(record);
        Assert.Equal(anchorKey, record!.RecordAnchorKey);
        Assert.Single(record.Participants);
        Assert.Equal(anchorKey, record.Participants[0].Key);
        Assert.True(record.Participants[0].Acked);
    }

    // ── A durable transaction may not modify an ephemeral key ──────────────────────────────────────────────

    [Fact]
    public async Task DurableCommit_RejectsEphemeralModifiedKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string persistentKey = "durable-eph:persistent";
        const string ephemeralKey  = "durable-eph:ephemeral";
        byte[] value = "x"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-eph");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        await WriteInTxn(node, handle, persistentKey, value, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, ephemeralKey, value, KeyValueDurability.Ephemeral);

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Aborted, commitResult);

        // No decision record was written for a rejected durable transaction.
        CoordinatorDecisionRecord? record = await WaitForDecision(node, txId, wantStatus: null, timeoutMs: 1_000);
        Assert.Null(record);
    }

    // ── A new durable transaction is rejected when the outstanding-decision budget is full ─────────────────

    [Fact]
    public async Task DurableCommit_OutstandingBudgetFull_RejectsNewDurableTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // Cap the durable-decision budget at one outstanding record and pre-fill it with an outstanding
        // (CommitDecided) record, so a new durable transaction is rejected rather than overshooting the budget.
        // The generous outcome-retention cap proves admission is governed by the dedicated budget, not that cache.
        await using EmbeddedKahunaNode node = new(EmbeddedOptions(outcomeRetentionMax: 10_000, durableOutstandingMax: 1));
        await node.StartAsync(ct);

        HLCTimestamp seedTx = new(1, 100, 1);
        CoordinatorDecisionRecord seed = new(
            seedTx, "seed-coord", "durable-cap:seed", seedTx, CoordinatorDecisionStatus.CommitDecided,
            [new CoordinatorParticipant("durable-cap:seed", KeyValueDurability.Persistent, HLCTimestamp.Zero, true, false)],
            [], seedTx, HLCTimestamp.Zero);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        const string anchorKey = "durable-cap:key";
        byte[] value = "y"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-cap");
        Assert.Equal(KeyValueResponseType.Set, startType);

        await WriteInTxn(node, handle, anchorKey, value, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Aborted, commitResult);

        // The refusal is counted for operators, and the anchor value never committed.
        Assert.True(Store(node).GetCapacityStats().Rejections >= 1);
        await AssertValueAbsent(node, anchorKey);
    }

    // ── Completed records held for the idempotency window do not consume durable-admission budget ───────────

    [Fact]
    public async Task DurableCommit_CompletedRecordsDoNotConsumeBudget_AdmitsNewDurableTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // Budget of one outstanding record, pre-filled with a COMPLETED record. Under the old rule (total record
        // count) this would block a new durable transaction; a completed record is evictable retention, so it must
        // not occupy the outstanding budget and the new transaction commits.
        await using EmbeddedKahunaNode node = new(EmbeddedOptions(durableOutstandingMax: 1));
        await node.StartAsync(ct);

        HLCTimestamp seedTx = new(1, 100, 1);
        CoordinatorDecisionRecord seed = new(
            seedTx, "seed-coord", "durable-done:seed", seedTx, CoordinatorDecisionStatus.Completed,
            [new CoordinatorParticipant("durable-done:seed", KeyValueDurability.Persistent, HLCTimestamp.Zero, true, true)],
            [], seedTx, seedTx);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        // The completed seed is present but does not count against the outstanding budget.
        var stats = Store(node).GetCapacityStats();
        Assert.Equal(0, stats.Outstanding);
        Assert.Equal(1, stats.Completed);

        const string anchorKey = "durable-done:key";
        byte[] value = "y"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-done");
        Assert.Equal(KeyValueResponseType.Set, startType);
        await WriteInTxn(node, handle, anchorKey, value, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitResult);
        await AssertValueCommitted(node, anchorKey, value);
    }

    private static async Task AssertValueAbsent(EmbeddedKahunaNode node, string key)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType r, ReadOnlyKeyValueEntry? e) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.True(r == KeyValueResponseType.DoesNotExist || e?.Value is null);
    }

    // ── A non-durable Completed transition holds the client at MustRetry until recovery finishes it ─────────

    [Fact]
    public async Task DurableCommit_CompletedTransitionNotDurable_ReportsMustRetryUntilRecoveryCompletes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey = "durable:progress:aaa";
        const string key2      = "durable:progress:bbb";
        const string key3      = "durable:progress:ccc";
        byte[] v1 = "one"u8.ToArray();
        byte[] v2 = "two"u8.ToArray();
        byte[] v3 = "three"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-progress-1");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        await WriteInTxn(node, handle, anchorKey, v1, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, key2, v2, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, key3, v3, KeyValueDurability.Persistent);

        // Block only the Completed transition from becoming durable. Participant acks (recorded under the
        // CommitDecided status) still persist, so every value commits — but the decision cannot reach Completed.
        Store(node).UpsertFault = rec => rec.Status == CoordinatorDecisionStatus.Completed;

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        // The client is told MustRetry, never Committed: a definite commit is only reported once the decision is
        // durably Completed.
        Assert.Equal(KeyValueResponseType.MustRetry, commitResult);

        // Every participant did commit, and the record is fully acknowledged but stuck below Completed.
        await AssertValueCommitted(node, anchorKey, v1);
        await AssertValueCommitted(node, key2, v2);
        await AssertValueCommitted(node, key3, v3);

        Assert.True(Store(node).TryGet(txId, out CoordinatorDecisionRecord stuck));
        Assert.Equal(CoordinatorDecisionStatus.CommitDecided, stuck.Status);
        Assert.True(stuck.AllParticipantsAcked);

        // Once the transition can persist again, recovery drives the outstanding record to Completed.
        Store(node).UpsertFault = null;
        await ((KahunaManager)node.Kahuna).RecoverOutstandingDecisions(TimeSpan.FromMinutes(5), ct);

        CoordinatorDecisionRecord? completed = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(completed);
        Assert.True(completed!.AllParticipantsAcked);
    }

    // ── A participant ack that never becomes durable keeps its completion receipt (its proof of commit) held ─

    [Fact]
    public async Task DurableCommit_AckUpsertNotDurable_KeepsCompletionReceiptUntilRecoveryCompletes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey = "durable:receipt:aaa";
        const string key2      = "durable:receipt:bbb";
        byte[] v1 = "one"u8.ToArray();
        byte[] v2 = "two"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-receipt-1");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        await WriteInTxn(node, handle, anchorKey, v1, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, key2, v2, KeyValueDurability.Persistent);

        // Block every progress upsert. The secondary commits on its participant (recording a completion receipt),
        // but its ack never becomes durable — so the receipt, the proof of that commit, must stay held: forgetting
        // it before the ack is durable would strand the record "unacked" with its proof gone, and recovery (which
        // re-drives only non-acked participants and resolves an already-applied commit through the still-held
        // receipt) could never complete it.
        Store(node).UpsertFault = _ => true;

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, commitResult);

        // The values committed regardless of the progress-write failure.
        await AssertValueCommitted(node, anchorKey, v1);
        await AssertValueCommitted(node, key2, v2);

        // The un-acknowledged participant's completion receipt is still present.
        CompletionReceiptStore receipts = ((KahunaManager)node.Kahuna).CompletionReceiptStore;
        Assert.True(receipts.Contains(txId, key2, KeyValueDurability.Persistent), "secondary receipt was forgotten before its ack was durable");

        // Once upserts can persist, recovery records the acks durably, then forgets the receipts and completes.
        Store(node).UpsertFault = null;
        await ((KahunaManager)node.Kahuna).RecoverOutstandingDecisions(TimeSpan.FromMinutes(5), ct);

        CoordinatorDecisionRecord? completed = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(completed);
        Assert.False(receipts.Contains(txId, key2, KeyValueDurability.Persistent), "receipt should be forgotten once the ack is durable");
    }

    // ── A held cleanup lock is not stranded on coordinator crash: recovery replays it and Completed waits ──

    [Fact]
    public async Task DurableCommit_CleanupLockHeldOnCrash_IsReplayedByRecovery_AndCompletedWaits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey = "durable:cleanup:aaa";
        const string lockKey   = "durable:cleanup:held";
        byte[] v1 = "one"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-cleanup-1");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        // Write the anchor (the sole participant) and hold an exclusive lock on a key it does not modify — a
        // non-modified point lock is a frozen cleanup effect, not a participant.
        await WriteInTxn(node, handle, anchorKey, v1, KeyValueDurability.Persistent);
        await AcquireLockInTxn(node, handle, lockKey, KeyValueDurability.Persistent);

        // Model a coordinator that dies right after the decision is durable but before it drives cleanup: block
        // every decision-store progress upsert. The anchor still commits and installs the decision (that path does
        // not upsert), so the transaction is durably committed, but the cleanup release is never recorded and the
        // lock stays held.
        Store(node).UpsertFault = _ => true;

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        // Completion waits on cleanup: the client is told MustRetry, never Committed, while the lock is outstanding.
        Assert.Equal(KeyValueResponseType.MustRetry, commitResult);
        await AssertValueCommitted(node, anchorKey, v1);

        Assert.True(Store(node).TryGet(txId, out CoordinatorDecisionRecord stuck));
        Assert.Equal(CoordinatorDecisionStatus.CommitDecided, stuck.Status);
        CoordinatorCleanupEffect stuckEffect = Assert.Single(stuck.CleanupEffects);
        Assert.Equal(CoordinatorCleanupKind.KeyRelease, stuckEffect.Kind);
        Assert.Equal(lockKey, stuckEffect.Key);
        Assert.False(stuckEffect.Released);

        // The lock really is still held: a different transaction cannot acquire it.
        Assert.Equal(KeyValueResponseType.AlreadyLocked, await ProbeExclusiveLock(node, "probe-held", lockKey, KeyValueDurability.Persistent));

        // Recovery replays the frozen cleanup: it releases the lock, records the release, and completes.
        Store(node).UpsertFault = null;
        await ((KahunaManager)node.Kahuna).RecoverOutstandingDecisions(TimeSpan.FromMinutes(5), ct);

        CoordinatorDecisionRecord? completed = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(completed);
        Assert.True(completed!.AllCleanupReleased);

        // The lock is now free: another transaction can acquire it.
        Assert.Equal(KeyValueResponseType.Locked, await ProbeExclusiveLock(node, "probe-free", lockKey, KeyValueDurability.Persistent));
    }

    // ── A durable commit inline-releases every frozen cleanup kind and reaches Completed with them released ──

    [Fact]
    public async Task DurableCommit_FreezesAndReleasesReadPrefixAndRangeCleanup_ReachingCompleted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey  = "durable:kinds:aaa";
        const string readKey    = "durable:kinds:read";
        const string prefixKey  = "durable:kinds:prefix/";
        const string rangePrefix = "durable:kinds:range";
        byte[] v1 = "one"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-kinds-1");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        await WriteInTxn(node, handle, anchorKey, v1, KeyValueDurability.Persistent);
        await ReadInTxn(node, handle, readKey, KeyValueDurability.Persistent);
        await AcquirePrefixLockInTxn(node, handle, prefixKey, KeyValueDurability.Persistent);
        await AcquireRangeLockInTxn(node, handle, rangePrefix, "a", true, "z", false, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, string? committedAnchor) =
            await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        // With every frozen cleanup effect releasable inline, the commit reaches a definite Committed and the
        // record is durably Completed with every effect released.
        Assert.Equal(KeyValueResponseType.Committed, commitResult);
        Assert.Equal(anchorKey, committedAnchor);

        CoordinatorDecisionRecord? completed = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(completed);
        Assert.True(completed!.AllCleanupReleased);

        // Every non-modified cleanup kind is present and released; the modified anchor is a participant, not a
        // cleanup effect.
        Assert.Contains(completed.CleanupEffects, e => e.Kind == CoordinatorCleanupKind.KeyRelease && e.Key == readKey);
        Assert.Contains(completed.CleanupEffects, e => e.Kind == CoordinatorCleanupKind.PrefixLock && e.Key == prefixKey);
        Assert.Contains(completed.CleanupEffects, e => e.Kind == CoordinatorCleanupKind.RangeLock && e.Key == rangePrefix);
        Assert.DoesNotContain(completed.CleanupEffects, e => e.Key == anchorKey);
    }

    // ── The initial record replicates to every node from the one committed anchor proposal ─────────────────

    private sealed record ClusterNode(RaftManager Raft, KahunaManager Kahuna);

    private (RaftManager, KahunaManager) BuildClusterNode(
        int nodeId, int port, string[] peers, MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        ILogger<IRaft>   raftLogger   = NullLogger<IRaft>.Instance;
        ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName             = "durabledecision" + nodeId,
            NodeId               = nodeId,
            Host                 = "localhost",
            Port                 = port,
            InitialPartitions    = 2,
            StartElectionTimeout = 50,
            EndElectionTimeout   = 150,
            ElectionTimeoutSeed  = 71000 + nodeId,
            EnableQuiescence     = false
        };

        RaftManager raft = new(
            raftCfg,
            new StaticDiscovery([new(peers[0]), new(peers[1])]),
            new InMemoryWAL(raftLogger),
            comm,
            new HybridLogicalClock(),
            raftLogger);

        Kahuna.Server.Configuration.KahunaConfiguration kahunaConfig = new()
        {
            HttpsCertificate         = "",
            HttpsCertificatePassword = "",
            LocksWorkers             = 8,
            KeyValueWorkers          = 8,
            BackgroundWriterWorkers  = 1,
            Storage                  = "memory",
            StoragePath              = "/tmp",
            StorageRevision          = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            Phase2CommitTimeout       = 5000,
            ScriptCacheExpiration     = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;
        raft.OnLeaderChanged       += kahuna.OnLeaderChanged;

        return (raft, kahuna);
    }

    [Fact]
    public async Task DurableCommit_MultiNode_ReplicatesInstalledDecisionToEveryNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9431", "localhost:9432"];
        string[] p2 = ["localhost:9430", "localhost:9432"];
        string[] p3 = ["localhost:9430", "localhost:9431"];

        (RaftManager r1, KahunaManager k1) = BuildClusterNode(1, 9430, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildClusterNode(2, 9431, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildClusterNode(3, 9432, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:9430", k1 }, { "localhost:9431", k2 }, { "localhost:9432", k3 } });
        comm.SetNodes(new() { { "localhost:9430", r1 }, { "localhost:9431", r2 }, { "localhost:9432", r3 } });

        ClusterNode[] nodes = [new(r1, k1), new(r2, k2), new(r3, k3)];
        try
        {
            await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

            for (int partition = 0; partition <= 1; partition++)
                while (!await r1.AmILeader(partition, ct) && !await r2.AmILeader(partition, ct) && !await r3.AmILeader(partition, ct))
                    await Task.Delay(50, ct);

            // Drive from partition 1's leader so the anchor commit applies locally and replicates to the peers.
            ClusterNode coord = nodes[0];
            foreach (ClusterNode n in nodes)
                if (await n.Raft.AmILeader(1, ct)) { coord = n; break; }

            const string anchorKey = "durable-repl:aaa";
            const string key2      = "durable-repl:bbb";
            byte[] v1 = "alpha"u8.ToArray();
            byte[] v2 = "beta"u8.ToArray();

            (KeyValueResponseType startType, TransactionHandle handle) = await coord.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey     = "durable-repl-coord",
                    Locking            = KeyValueTransactionLocking.Pessimistic,
                    DecisionDurability = DecisionDurability.Durable,
                    AsyncRelease       = false,
                    Timeout            = 10_000,
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            HLCTimestamp txId = handle.TransactionId;

            foreach ((string key, byte[] value) in new[] { (anchorKey, v1), (key2, v2) })
            {
                (KeyValueResponseType s, _, _) = await RetrySet(() =>
                    coord.Kahuna.LocateAndTrySetKeyValue(txId, key, value, null, -1, KeyValueFlags.None, 0,
                        KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey,
                        operationId: TransactionOperationId.NewRandom()));
                Assert.Equal(KeyValueResponseType.Set, s);
            }

            (KeyValueResponseType commitResult, string? committedAnchor) =
                await coord.Kahuna.LocateAndCommitTransaction(handle, ct);

            // The anchor commit installs and replicates the decision regardless of whether this coordinator node
            // also leads the anchor's data partition. When it does, the progress upserts persist locally and the
            // commit reports Committed; when it does not, the progress delta cannot be replicated from a follower,
            // so the commit reports MustRetry and leaves the anchor leader's recovery driver to complete the
            // decision — it never falsely reports Committed on a non-durable progress write. Either way the initial
            // CommitDecided record — the point of this test — rides the one committed anchor proposal to every node.
            Assert.Contains(commitResult, new[] { KeyValueResponseType.Committed, KeyValueResponseType.MustRetry });
            if (commitResult == KeyValueResponseType.Committed)
                Assert.Equal(anchorKey, committedAnchor);

            // The initial CommitDecided record — the frozen participant set with the anchor already acknowledged
            // — rides the one committed anchor proposal and replicates to every node.
            long deadline = Environment.TickCount64 + 10_000;
            while (Environment.TickCount64 < deadline &&
                   nodes.Any(n => !n.Kahuna.CoordinatorDecisionStore.TryGet(txId, out _)))
                await Task.Delay(50, ct);

            foreach (ClusterNode n in nodes)
            {
                Assert.True(n.Kahuna.CoordinatorDecisionStore.TryGet(txId, out CoordinatorDecisionRecord rec),
                    "a node is missing the replicated decision record");
                Assert.Equal(anchorKey, rec.RecordAnchorKey);
                Assert.Equal(2, rec.Participants.Count);
                Assert.Contains(rec.Participants, p => p.Key == anchorKey && p.Acked);
                Assert.Contains(rec.Participants, p => p.Key == key2);
            }
        }
        finally
        {
            foreach (ClusterNode n in nodes)
            {
                try { await n.Raft.LeaveCluster(dispose: true); }
                catch (ObjectDisposedException) { }
            }
        }
    }
}

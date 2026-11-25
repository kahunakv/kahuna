using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// What the operations on key-actor state wait for on one node while the delivery of a committed batch is held: staged
/// writes, lock grants, lock releases, the checks of a commit and ephemeral tickets. One that names its keys waits only
/// when the batch can change one of them. One that covers a range waits for everything.
/// </summary>
public sealed class TestKeyScopedApplyWaitActorOperations : KeyScopedApplyWaitTestBase
{
    private const string Written = "wait/written";

    private const string Other = "wait/other";

    private const string Confirm = "kahuna.kv.mutation.confirm";

    private const string UndeliveredKey = "kahuna.kv.mutation.undelivered_key";

    public TestKeyScopedApplyWaitActorOperations(ITestOutputHelper outputHelper) : base(outputHelper)
    {
    }

    /// <summary>The operations that name one key.</summary>
    public enum KeyOperation
    {
        Set,
        SetMany,
        Delete,
        DeleteMany,
        Extend,
        PointLock,
        ManyPointLocks,
        SharedRangeLock,
        ExclusiveRangeLock,
        LockedProbe,
        ReleasePointLock,
        ReleaseManyPointLocks,
        ReleaseRangeLock,
        CommitCheck,
        ManyCommitChecks
    }

    /// <summary>The operations that cover keys they do not name, or that must see everything before a copy.</summary>
    public enum RangeOperation
    {
        PrefixLock,
        SharedRangeLock,
        ExclusiveRangeLock,
        RangeLockWithOpenStart,
        RangeLockWithOpenEnd,
        UnboundedRangeLock,
        WriteFenceOnOneKey,
        LockedRangeProbe,
        ReleasePrefixLock,
        ReleaseRangeLock,
        ReleaseUnboundedRangeLock
    }

    /// <summary>What an operation answered. <paramref name="Revision"/> is the revision it staged or observed, or -1
    /// when the operation reports none.</summary>
    private sealed record Answer(KeyValueResponseType Type, long Revision = -1, string? Value = null)
    {
        public bool Served => Type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.Errored or KeyValueResponseType.WaitingForReplication);
    }

    private static async Task<Answer> Perform(EmbeddedKahunaNode node, KeyOperation operation, HLCTimestamp tx, string key, long expectedBase, CancellationToken ct)
    {
        KahunaManager kahuna = ManagerOf(node);
        const KeyValueDurability durability = KeyValueDurability.Persistent;

        switch (operation)
        {
            case KeyOperation.Set:
            {
                (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
                    tx, key, "staged"u8.ToArray(), null, -1, KeyValueFlags.None, 0, durability, ct);
                return new(type, revision);
            }

            case KeyOperation.SetMany:
            {
                KahunaSetKeyValueResponseItem item = Assert.Single(await kahuna.LocateAndTrySetManyKeyValue(
                    [new() { TransactionId = tx, Key = key, Value = "staged"u8.ToArray(), CompareRevision = -1, Durability = durability }], ct));
                return new(item.Type, item.Revision);
            }

            case KeyOperation.Delete:
            {
                (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTryDeleteKeyValue(tx, key, durability, ct);
                return new(type, revision);
            }

            case KeyOperation.DeleteMany:
            {
                KahunaDeleteKeyValueResponseItem item = Assert.Single(await kahuna.LocateAndTryDeleteManyKeyValue(
                    [new() { TransactionId = tx, Key = key, Durability = durability }], ct));
                return new(item.Type, item.Revision);
            }

            case KeyOperation.Extend:
            {
                (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTryExtendKeyValue(tx, key, 60_000, durability, ct);
                return new(type, revision);
            }

            case KeyOperation.PointLock:
            {
                (KeyValueResponseType type, _, _, _, long baseRevision) = await kahuna.LocateAndTryAcquireExclusiveLockObserved(tx, key, 30_000, durability, ct);
                return new(type, baseRevision);
            }

            case KeyOperation.ManyPointLocks:
            {
                (KeyValueResponseType type, _, _, _, long baseRevision) = Assert.Single(
                    await kahuna.LocateAndTryAcquireManyExclusiveLocksObserved(tx, [(key, 30_000, durability)], ct));
                return new(type, baseRevision);
            }

            case KeyOperation.SharedRangeLock:
            case KeyOperation.ExclusiveRangeLock:
            {
                (KeyValueResponseType type, _) = await kahuna.LocateAndTryAcquireRangeLock(
                    tx, "wait", key, true, key, true, 30_000, durability,
                    operation == KeyOperation.SharedRangeLock ? RangeLockMode.Shared : RangeLockMode.Exclusive, ct);
                return new(type);
            }

            case KeyOperation.LockedProbe:
            {
                KeyValueProbeResult probe = Assert.Single(await kahuna.LocateAndTryProbeMany(
                    tx, HLCTimestamp.Zero, [key], KeyValueProbeLock.Shared, 30_000, durability, ct));
                return new(probe.Type, probe.Entry?.Revision ?? -1, probe.Entry?.Value is { } value ? Encoding.UTF8.GetString(value) : null);
            }

            case KeyOperation.ReleasePointLock:
                return new((await kahuna.LocateAndTryReleaseExclusiveLock(tx, key, durability, ct)).Item1);

            case KeyOperation.ReleaseManyPointLocks:
                return new(Assert.Single(await kahuna.LocateAndTryReleaseManyExclusiveLocks(tx, [(key, durability)], ct)).Item1);

            case KeyOperation.ReleaseRangeLock:
                return new(await kahuna.LocateAndTryReleaseExclusiveRangeLock(tx, "wait", key, true, key, true, durability, ct));

            case KeyOperation.CommitCheck:
                return new(await kahuna.LocateAndTryCheckWriteIntent(tx, key, durability, ct));

            default:
                return new(Assert.Single(await kahuna.LocateAndTryCheckManyWriteIntents(
                    tx,
                    [new(key, durability, KeyValueConflictChecks.ForeignRangeLock | KeyValueConflictChecks.WriteIntent | KeyValueConflictChecks.StagedBase, expectedBase)],
                    ct)).type);
        }
    }

    private static async Task<Answer> Perform(EmbeddedKahunaNode node, RangeOperation operation, HLCTimestamp tx, string space, CancellationToken ct)
    {
        KahunaManager kahuna = ManagerOf(node);
        const KeyValueDurability durability = KeyValueDurability.Persistent;
        string first = space + "/a", last = space + "/z";

        async Task<Answer> RangeLock(string? start, bool startInclusive, string? end, bool endInclusive, RangeLockMode mode) =>
            new((await kahuna.LocateAndTryAcquireRangeLock(tx, space, start, startInclusive, end, endInclusive, 30_000, durability, mode, ct)).Item1);

        return operation switch
        {
            RangeOperation.PrefixLock => new(await kahuna.LocateAndTryAcquireExclusivePrefixLock(tx, space, 30_000, durability, ct)),
            RangeOperation.SharedRangeLock => await RangeLock(first, true, last, true, RangeLockMode.Shared),
            RangeOperation.ExclusiveRangeLock => await RangeLock(first, true, last, true, RangeLockMode.Exclusive),
            RangeOperation.RangeLockWithOpenStart => await RangeLock(first, false, first, true, RangeLockMode.Shared),
            RangeOperation.RangeLockWithOpenEnd => await RangeLock(first, true, first, false, RangeLockMode.Shared),
            RangeOperation.UnboundedRangeLock => await RangeLock(null, true, null, true, RangeLockMode.Shared),
            RangeOperation.WriteFenceOnOneKey => await RangeLock(first, true, first, true, RangeLockMode.WriteFence),
            RangeOperation.LockedRangeProbe => new(Assert.Single(await kahuna.LocateAndTryProbeManyRanges(
                tx, HLCTimestamp.Zero, space, [new KeyValueProbeRange(first, true, last, true, 10)], KeyValueProbeLock.Shared, 30_000, durability, ct)).Type),
            RangeOperation.ReleasePrefixLock => new(await kahuna.LocateAndTryReleaseExclusivePrefixLock(tx, space, durability, ct)),
            RangeOperation.ReleaseRangeLock => new(await kahuna.LocateAndTryReleaseExclusiveRangeLock(tx, space, first, true, last, true, durability, ct)),
            _ => new(await kahuna.LocateAndTryReleaseExclusiveRangeLock(tx, space, null, true, null, true, durability, ct))
        };
    }

    public static TheoryData<KeyOperation> KeyOperations => [.. Enum.GetValues<KeyOperation>()];

    /// <summary>Operations that the hold must keep back. A wait that outlives the confirmation timeout answers nothing,
    /// which counts as held.</summary>
    private static async Task AssertWait(params (string Operation, Task<Answer> Answer)[] waiting)
    {
        await Task.Delay(Pending, TestContext.Current.CancellationToken);

        foreach ((string operation, Task<Answer> answer) in waiting)
        {
            if (answer.IsCompleted)
                Assert.False((await answer).Served, $"{operation} answered {await answer} while the delivery was held");
        }
    }

    /// <summary>What an operation that saw the head at <paramref name="head"/> answers.</summary>
    private static void AssertJudgedOn(KeyOperation operation, Answer answer, long head)
    {
        switch (operation)
        {
            case KeyOperation.Set or KeyOperation.SetMany:
                Assert.Equal(new Answer(KeyValueResponseType.Set, head + 1), answer);
                break;

            case KeyOperation.Delete or KeyOperation.DeleteMany:
                Assert.Equal(new Answer(KeyValueResponseType.Deleted, head + 1), answer);
                break;

            case KeyOperation.Extend:
                Assert.Equal(new Answer(KeyValueResponseType.Extended, head), answer);
                break;

            case KeyOperation.PointLock or KeyOperation.ManyPointLocks:
                Assert.Equal(new Answer(KeyValueResponseType.Locked, head), answer);
                break;

            case KeyOperation.SharedRangeLock or KeyOperation.ExclusiveRangeLock:
                Assert.Equal(KeyValueResponseType.Locked, answer.Type);
                break;

            case KeyOperation.LockedProbe:
                Assert.Equal(KeyValueResponseType.Get, answer.Type);
                Assert.Equal(head, answer.Revision);
                break;

            case KeyOperation.ReleasePointLock or KeyOperation.ReleaseManyPointLocks or KeyOperation.ReleaseRangeLock:
                Assert.True(TransactionCoordinator.IsReleaseAcked(answer.Type), $"the release answered {answer.Type}");
                break;

            default:
                Assert.Equal(KeyValueResponseType.DoesNotExist, answer.Type);
                break;
        }
    }

    private static async Task<Answer> PerformUntilServed(EmbeddedKahunaNode node, KeyOperation operation, HLCTimestamp tx, string key, long expectedBase, CancellationToken ct)
    {
        Answer answer = await Perform(node, operation, tx, key, expectedBase, ct);

        for (int attempt = 0; attempt < 100 && !answer.Served; attempt++)
        {
            await Task.Delay(20, ct);
            answer = await Perform(node, operation, tx, key, expectedBase, ct);
        }

        return answer;
    }

    private static async Task<long> RevisionOf(EmbeddedKahunaNode node, string key, CancellationToken ct) =>
        (await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)).Item2!.Revision;

    [Theory]
    [MemberData(nameof(KeyOperations))]
    public async Task AnUndeliveredWrite_HoldsBackAnOperationOnItsKey_AndOnNoOther(KeyOperation operation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        await Seed(node, "old", ct, Written, Other);
        long otherHead = await RevisionOf(node, Other, ct);
        long oldHead = await RevisionOf(node, Written, ct);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);

        // Acknowledged at quorum, with its entry not delivered.
        await Set(node.Kahuna, Written, "new", ct);
        Assert.True(Marked(node, Written));

        // A transaction of its own for each attempt: an attempt that met a lapsed lease is still waiting.
        Answer other = await Promptly(() => Perform(node, operation, Now(node), Other, otherHead, ct));
        AssertJudgedOn(operation, other, otherHead);

        HLCTimestamp tx = Now(node);
        Task<Answer> waiting = Perform(node, operation, tx, Written, oldHead + 1, ct);
        await AssertWait((operation.ToString(), waiting));

        await held.Resume();

        Answer answer = await waiting;
        if (!answer.Served)
            answer = await PerformUntilServed(node, operation, tx, Written, oldHead + 1, ct);

        AssertJudgedOn(operation, answer, oldHead + 1);
        if (operation == KeyOperation.LockedProbe)
            Assert.Equal("new", answer.Value);
    }

    [Fact]
    public async Task WithTheSwitchOff_AnOperationOnAnotherKeyWaitsToo()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, static options => options.KeyScopedApplyWait = false);

        // A key of its own for each operation, so that none of them meets another.
        KeyOperation[] operations = Enum.GetValues<KeyOperation>();
        string[] keys = [.. operations.Select(static operation => "wait/other-" + operation)];
        await Seed(node, "old", ct, [Written, .. keys]);

        long[] heads = new long[keys.Length];
        for (int i = 0; i < keys.Length; i++)
            heads[i] = await RevisionOf(node, keys[i], ct);

        long term = node.Raft.GetPartitionTerm(PartitionOf(node, Written));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);
        await Set(node.Kahuna, Written, "new", ct);

        (string Operation, Task<Answer> Answer)[] waiting =
        [
            .. operations.Select((operation, i) => (operation.ToString(), Perform(node, operation, Now(node), keys[i], heads[i], ct))),
            ("LeaderTerm", Task.Run(async () => new Answer(Assert.Single(await node.Kahuna.LocateAndTryCheckManyWriteIntents(
                Now(node), [new(Other, KeyValueDurability.Persistent, KeyValueConflictChecks.LeaderTerm, term)], ct)).type), ct))
        ];
        await AssertWait(waiting);

        await held.Resume();
        await Task.WhenAll(waiting.Select(static operation => operation.Answer));
    }

    [Fact]
    public async Task AnOperationOverARange_WaitsForEverything()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        // A key space of its own for each operation, so that none of them meets another.
        RangeOperation[] operations = Enum.GetValues<RangeOperation>();
        await Seed(node, "old", ct, [Written, .. operations.Select(static operation => $"wait{operation}/a")]);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);
        await Set(node.Kahuna, Written, "new", ct);

        (string Operation, Task<Answer> Answer)[] waiting =
            [.. operations.Select(operation => (operation.ToString(), Perform(node, operation, Now(node), "wait" + operation, ct)))];
        await AssertWait(waiting);

        await held.Resume();

        foreach ((string operation, Task<Answer> answer) in waiting)
        {
            KeyValueResponseType type = (await answer).Type;

            Assert.True(
                type is KeyValueResponseType.Locked or KeyValueResponseType.Get or KeyValueResponseType.MustRetry || TransactionCoordinator.IsReleaseAcked(type),
                $"{operation} answered {type} after the delivery");
        }
    }

    /// <summary>
    /// A page that a range move copied here names no keys, and the mover is told that it is durable before this node
    /// delivered it. An operation that routes here after the cutover must not be answered from a state without it.
    /// </summary>
    [Fact]
    public async Task AnUndeliveredRangePage_HoldsBackAnOperationOnAnyKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        await Seed(node, "other", ct, Other);
        int partition = PartitionOf(node, Other);

        KeyOperation[] operations = Enum.GetValues<KeyOperation>();
        string[] keys = [.. operations.Select(static operation => "wait/moved-" + operation)];

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, partition, ct);

        HLCTimestamp now = Now(node);
        using MemoryStream page = new();
        KvStateMachineTransfer.WritePage(
            page, [.. keys.Select(key => (key, new ReadOnlyKeyValueEntry("moved"u8.ToArray(), 7, HLCTimestamp.Zero, now, now, KeyValueState.Set)))], hasMore: false);
        Assert.True(await node.Kahuna.ReplicateKeyValueRangePageLocal(partition, page.ToArray(), ct));

        HLCTimestamp[] transactions = [.. operations.Select(_ => Now(node))];
        (string Operation, Task<Answer> Answer)[] waiting =
            [.. operations.Select((operation, i) => (operation.ToString(), Perform(node, operation, transactions[i], keys[i], 7, ct)))];
        await AssertWait(waiting);

        await held.Resume();

        for (int i = 0; i < operations.Length; i++)
        {
            Answer answer = await waiting[i].Answer;
            AssertJudgedOn(operations[i], answer.Served ? answer : await PerformUntilServed(node, operations[i], transactions[i], keys[i], 7, ct), 7);
        }
    }

    [Fact]
    public async Task ABatchOfWritesOrLocks_WaitsWhenOneOfItsKeysIsNotDelivered()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = ManagerOf(node);

        string[] keys = [.. Enumerable.Range(0, 8).Select(static i => "wait/k" + i)];
        await Seed(node, "old", ct, keys);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, keys[0]), ct);
        await Set(kahuna, keys[7], "new", ct);

        Task<List<KahunaSetKeyValueResponseItem>> SetMany(IEnumerable<string> batch, HLCTimestamp tx) => kahuna.LocateAndTrySetManyKeyValue(
            [.. batch.Select(key => new KahunaSetKeyValueRequestItem { TransactionId = tx, Key = key, Value = "staged"u8.ToArray(), CompareRevision = -1, Durability = KeyValueDurability.Persistent })], ct);

        Task<List<KahunaDeleteKeyValueResponseItem>> DeleteMany(IEnumerable<string> batch, HLCTimestamp tx) => kahuna.LocateAndTryDeleteManyKeyValue(
            [.. batch.Select(key => new KahunaDeleteKeyValueRequestItem { TransactionId = tx, Key = key, Durability = KeyValueDurability.Persistent })], ct);

        Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>> LockMany(IEnumerable<string> batch, HLCTimestamp tx) => kahuna.LocateAndTryAcquireManyExclusiveLocks(
            tx, [.. batch.Select(static key => (key, 30_000, KeyValueDurability.Persistent))], ct);

        Task<KeyValueProbeResult[]> Probe(string[] batch, HLCTimestamp tx) => kahuna.LocateAndTryProbeMany(
            tx, HLCTimestamp.Zero, batch, KeyValueProbeLock.Shared, 30_000, KeyValueDurability.Persistent, ct);

        Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> Check(IEnumerable<string> batch, HLCTimestamp tx) => kahuna.LocateAndTryCheckManyWriteIntents(
            tx, [.. batch.Select(static key => new KeyValueConflictProbe(key, KeyValueDurability.Persistent, KeyValueConflictChecks.WriteIntent))], ct);

        // Two keys for each operation, all without the written one.
        Assert.All(await Promptly(() => SetMany(keys[..2], Now(node))), static item => Assert.True(item.Type is KeyValueResponseType.Set or KeyValueResponseType.MustRetry, item.Type.ToString()));
        Assert.All(await Promptly(() => DeleteMany(keys[2..4], Now(node))), static item => Assert.True(item.Type is KeyValueResponseType.Deleted or KeyValueResponseType.MustRetry, item.Type.ToString()));
        Assert.Equal(KeyValueResponseType.Locked, (await Promptly(() => LockMany(keys[4..5], Now(node))))[0].Item1);
        Assert.True((await Promptly(() => Probe(keys[5..7], Now(node))))[0].Locked);
        Assert.All(await Promptly(() => Check(keys[5..7], Now(node))), static answer => Assert.Equal(KeyValueResponseType.DoesNotExist, answer.type));

        Task[] waiting =
        [
            SetMany([keys[6], keys[7]], Now(node)),
            DeleteMany([keys[6], keys[7]], Now(node)),
            LockMany([keys[6], keys[7]], Now(node)),
            Probe([keys[6], keys[7]], Now(node)),
            Check([keys[6], keys[7]], Now(node))
        ];

        await Task.Delay(Pending, ct);
        Assert.All(waiting, static task => Assert.False(task.IsCompleted));

        await held.Resume();
        await Task.WhenAll(waiting);
    }

    [Fact]
    public async Task ABatchOfMoreKeysThanAreAsked_WaitsForTheWholeFrontier()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = ManagerOf(node);

        string[] keys = [.. Enumerable.Range(0, 513).Select(static i => "wait/k" + i)];
        await Seed(node, "old", ct, Written);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);
        await Set(kahuna, Written, "new", ct);

        Task<List<KahunaSetKeyValueResponseItem>> SetMany(string[] batch, HLCTimestamp tx) => kahuna.LocateAndTrySetManyKeyValue(
            [.. batch.Select(key => new KahunaSetKeyValueRequestItem { TransactionId = tx, Key = key, Value = "staged"u8.ToArray(), CompareRevision = -1, Durability = KeyValueDurability.Persistent })], ct);

        Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> Check(string[] batch) => kahuna.LocateAndTryCheckManyWriteIntents(
            Now(node), [.. batch.Select(static key => new KeyValueConflictProbe(key, KeyValueDurability.Persistent, KeyValueConflictChecks.ForeignRangeLock))], ct);

        HLCTimestamp tx = Now(node);
        Assert.Equal(512, (await Promptly(() => SetMany(keys[..512], tx))).Count(static item => item.Type == KeyValueResponseType.Set));
        Assert.Equal(512, (await Promptly(() => Check(keys[..512]))).Count);

        Task<List<KahunaSetKeyValueResponseItem>> set = SetMany(keys, tx);
        Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> check = Check(keys);
        await Task.Delay(Pending, ct);
        Assert.False(set.IsCompleted);
        Assert.False(check.IsCompleted);

        await held.Resume();
        Assert.Equal(513, (await set).Count);
        Assert.Equal(513, (await check).Count);
    }

    /// <summary>A staged write on an undelivered key must not take the revision that the undelivered write has.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ATransactionThatWritesAnUndeliveredKey_CommitsOnTopOfIt(bool locked)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = ManagerOf(node);

        await Seed(node, "old", ct, Written);
        long oldHead = await RevisionOf(node, Written, ct);
        TransactionHandle tx = await Start(kahuna, Other, ct, locked ? KeyValueTransactionLocking.Pessimistic : KeyValueTransactionLocking.Optimistic);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);
        await Set(kahuna, Written, "new", ct);

        Task staging = Task.Run(async () =>
        {
            if (locked)
            {
                (KeyValueResponseType type, _, _, _) = await RetryOnMustRetryAsync(() => kahuna.LocateAndTryAcquireExclusiveLock(
                    tx.TransactionId, Written, 30_000, KeyValueDurability.Persistent, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom()), static answer => answer.Item1);
                Assert.Equal(KeyValueResponseType.Locked, type);
            }

            (KeyValueResponseType set, long revision, _) = await RetryOnMustRetryAsync(() => kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, Written, "staged"u8.ToArray(), "new"u8.ToArray(), -1, KeyValueFlags.SetIfEqualToValue, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()), static answer => answer.Item1);
            Assert.Equal((KeyValueResponseType.Set, oldHead + 2), (set, revision));
        }, ct);

        await Task.Delay(Pending, ct);
        Assert.False(staging.IsCompleted);

        await held.Resume();
        await staging;

        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal("staged", await Get(kahuna, Written, ct));
        Assert.Equal(oldHead + 2, await RevisionOf(node, Written, ct));
    }

    /// <summary>Retries a commit that answered nothing: an attempt that met a lapsed lease waits for the delivery.</summary>
    private static Task<KeyValueResponseType> Commit(IKahuna kahuna, TransactionHandle tx, CancellationToken ct) => Task.Run(async () =>
    {
        while (true)
        {
            KeyValueResponseType type = (await kahuna.LocateAndCommitTransaction(tx, ct)).Type;

            if (type != KeyValueResponseType.MustRetry)
                return type;

            await Task.Delay(10, ct);
        }
    }, ct);

    /// <summary>
    /// The checks that a commit runs before it proposes: the held staged intents, the foreign range locks, the read set
    /// and, with locks, the proof that the locks are in force. None of them waits for a write of another key.
    /// </summary>
    [Theory]
    [InlineData(KeyValueTransactionLocking.Optimistic, true)]
    [InlineData(KeyValueTransactionLocking.Optimistic, false)]
    [InlineData(KeyValueTransactionLocking.Pessimistic, true)]
    [InlineData(KeyValueTransactionLocking.Pessimistic, false)]
    public async Task WhileAnotherWriteIsUndelivered_ACommitReachesItsProposal(KeyValueTransactionLocking locking, bool onePhase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, options =>
        {
            options.OnePhaseApplyTimeValidation = onePhase;
            options.DurableMaterializeOnResolve = onePhase;
        });
        KahunaManager kahuna = ManagerOf(node);

        await Seed(node, "old", ct, "wait/a", "wait/b", "wait/r", Written);

        TransactionHandle tx = await Start(kahuna, "wait/a", ct, locking);

        if (locking == KeyValueTransactionLocking.Pessimistic)
        {
            foreach (string key in (string[])["wait/a", "wait/b", "wait/r"])
            {
                (KeyValueResponseType type, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                    tx.TransactionId, key, 30_000, KeyValueDurability.Persistent, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Locked, type);
            }
        }

        (KeyValueResponseType read, _) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, "wait/r", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, read);

        await Stage(kahuna, tx, "wait/a", "new", ct);
        await Stage(kahuna, tx, "wait/b", "new", ct);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);
        await Set(kahuna, Written, "new", ct);
        Assert.False(Marked(node, "wait/a") || Marked(node, "wait/b") || Marked(node, "wait/r"));

        Task<KeyValueResponseType> commit = Commit(kahuna, tx, ct);

        // The marks of its keys are set when the scheduler dispatches its first batch, which is after the checks.
        await WaitUntilAsync(() => Marked(node, "wait/a") && Marked(node, "wait/b"));
        Assert.False(commit.IsCompleted);

        await held.Resume();

        Assert.Equal(KeyValueResponseType.Committed, await commit);
        Assert.Equal("new", await Get(kahuna, "wait/a", ct));
        Assert.Equal("new", await Get(kahuna, "wait/b", ct));
    }

    /// <summary>A transaction that only read under locks on its keys proposes nothing, so nothing of it waits.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WhileAnotherWriteIsUndelivered_ALockedReadCommitsAndReleases(bool pointLocks)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = ManagerOf(node);

        await Seed(node, "old", ct, "wait/a", "wait/b", Written);
        TransactionHandle tx = await Start(kahuna, "wait/a", ct, KeyValueTransactionLocking.Pessimistic);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);
        await Set(kahuna, Written, "new", ct);

        if (pointLocks)
        {
            Assert.Equal(KeyValueResponseType.Locked, (await Promptly(() => kahuna.LocateAndTryAcquireManyExclusiveLocks(
                tx.TransactionId, [("wait/a", 30_000, KeyValueDurability.Persistent), ("wait/b", 30_000, KeyValueDurability.Persistent)], ct,
                tx.CoordinatorKey, TransactionOperationId.NewRandom())))[1].Item1);
        }
        else
        {
            Assert.True((await Promptly(() => kahuna.LocateAndTryProbeMany(
                tx.TransactionId, HLCTimestamp.Zero, ["wait/a", "wait/b"], KeyValueProbeLock.Shared, 30_000, KeyValueDurability.Persistent, ct,
                tx.CoordinatorKey, TransactionOperationId.NewRandom())))[1].Locked);
        }

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct).WaitAsync(TimeSpan.FromSeconds(20), ct));

        // The locks are gone: another transaction takes both keys.
        HLCTimestamp next = Now(node);
        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> taken = await Promptly(() => kahuna.LocateAndTryAcquireManyExclusiveLocks(
            next, [("wait/a", 30_000, KeyValueDurability.Persistent), ("wait/b", 30_000, KeyValueDurability.Persistent)], ct));
        Assert.Equal([KeyValueResponseType.Locked, KeyValueResponseType.Locked], taken.Select(static answer => answer.Item1));
        Assert.True(Marked(node, Written));
    }

    /// <summary>
    /// The proof that a lock is still in force asks the leader for its term. No entry changes that answer, so the proof
    /// does not wait for the key it routes by.
    /// </summary>
    [Fact]
    public async Task TheProofOfALock_WaitsForNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        await Seed(node, "old", ct, Written, Other);
        long term = node.Raft.GetPartitionTerm(PartitionOf(node, Written));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, Written), ct);
        await Set(node.Kahuna, Written, "new", ct);

        // Beside it a check of another key, which must not wait for the key of the proof either.
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> answers = await Promptly(() => node.Kahuna.LocateAndTryCheckManyWriteIntents(
            Now(node),
            [
                new(Written, KeyValueDurability.Persistent, KeyValueConflictChecks.LeaderTerm, term),
                new(Other, KeyValueDurability.Persistent, KeyValueConflictChecks.WriteIntent)
            ], ct));
        Assert.Equal([KeyValueResponseType.DoesNotExist, KeyValueResponseType.DoesNotExist], answers.Select(static answer => answer.type));

        Assert.Equal(KeyValueResponseType.Aborted, Assert.Single(await Promptly(() => node.Kahuna.LocateAndTryCheckManyWriteIntents(
            Now(node), [new(Written, KeyValueDurability.Persistent, KeyValueConflictChecks.LeaderTerm, term - 1)], ct))).type);
        Assert.True(Marked(node, Written));
    }

    /// <summary>The tickets of an ephemeral mutation, one at a time and in a batch.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EphemeralTickets_WaitForNothing(bool commit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = ManagerOf(node);
        const KeyValueDurability durability = KeyValueDurability.Ephemeral;

        string[] keys = ["wait/a", "wait/b", "wait/c"];
        await Seed(node, "old", ct, keys);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, keys[0]), ct);

        foreach (string key in keys)
            await Set(kahuna, key, "new", ct);

        HLCTimestamp tx = Now(node);

        foreach (string key in keys)
        {
            Assert.Equal(KeyValueResponseType.Set, (await Promptly(() => kahuna.LocateAndTrySetKeyValue(
                tx, key, "ephemeral"u8.ToArray(), null, -1, KeyValueFlags.None, 0, durability, ct))).Item1);
        }

        HLCTimestamp commitId = Now(node);

        (KeyValueResponseType prepared, HLCTimestamp ticket, _, _) = await Promptly(() => kahuna.LocateAndTryPrepareMutations(tx, commitId, keys[0], durability, ct));
        Assert.Equal(KeyValueResponseType.Prepared, prepared);

        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> preparedMany = await Promptly(() => kahuna.LocateAndTryPrepareManyMutations(
            tx, commitId, [(keys[1], durability), (keys[2], durability)], ct));
        Assert.All(preparedMany, static answer => Assert.Equal(KeyValueResponseType.Prepared, answer.Item1));

        List<(string, HLCTimestamp, KeyValueDurability)> tickets = [.. preparedMany.Select(static answer => (answer.Item3, answer.Item2, answer.Item4))];
        KeyValueResponseType decided = commit ? KeyValueResponseType.Committed : KeyValueResponseType.RolledBack;

        KeyValueResponseType one = commit
            ? (await Promptly(() => kahuna.LocateAndTryCommitMutations(tx, keys[0], ticket, durability, ct))).Item1
            : (await Promptly(() => kahuna.LocateAndTryRollbackMutations(tx, keys[0], ticket, durability, ct))).Item1;
        Assert.Equal(decided, one);

        List<(KeyValueResponseType, string, long, KeyValueDurability)> many = commit
            ? await Promptly(() => kahuna.LocateAndTryCommitManyMutations(tx, tickets, ct))
            : await Promptly(() => kahuna.LocateAndTryRollbackManyMutations(tx, tickets, ct));
        Assert.Equal([decided, decided], many.Select(static answer => answer.Item1));

        Assert.True(keys.All(key => Marked(node, key)));
    }

    /// <summary>The persistent key of the same name is the one the log can change.</summary>
    [Fact]
    public async Task AnEphemeralTransaction_WaitsForNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = ManagerOf(node);

        await Seed(node, "old", ct, "wait/a", "wait/b");
        TransactionHandle tx = await Start(kahuna, "wait/a", ct);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, "wait/a"), ct);
        await Set(kahuna, "wait/a", "new", ct);
        await Set(kahuna, "wait/b", "new", ct);

        foreach (string key in (string[])["wait/a", "wait/b"])
        {
            (KeyValueResponseType type, _, _) = await Promptly(() => kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, "ephemeral"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Ephemeral, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct).WaitAsync(TimeSpan.FromSeconds(20), ct));

        (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await Promptly(() => kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wait/b", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct).AsTask());
        Assert.Equal(KeyValueResponseType.Get, read);
        Assert.Equal("ephemeral", Encoding.UTF8.GetString(entry!.Value!));
        Assert.True(Marked(node, "wait/a"));
    }

    /// <summary>A script over one ephemeral key runs in one turn of its actor, behind one confirmation. One that takes
    /// its transaction in hand uses the lock and the tickets.</summary>
    [Theory]
    [InlineData("ESET `wait/a` 'one'\nESET `wait/a` 'two'\nRETURN 1", true)]
    [InlineData("BEGIN ESET `wait/a` 'one' ESET `wait/a` 'two' COMMIT END", false)]
    public async Task AnEphemeralScript_WaitsForNothing(string script, bool inOneTurn)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = ManagerOf(node);

        await Seed(node, "old", ct, "wait/a");

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, "wait/a"), ct);
        await Set(kahuna, "wait/a", "new", ct);

        long turns = DurableTransactionMetrics.ScriptActorTurnsCount;
        KeyValueTransactionResult result = await Promptly(() => kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null));
        Assert.True(result.Type is KeyValueResponseType.Get or KeyValueResponseType.Set, $"{result.Type} {result.Reason}");
        Assert.True(Marked(node, "wait/a"));

        // Other nodes of this process count their turns too, so the count can only be asked to have grown.
        if (inOneTurn)
            Assert.True(DurableTransactionMetrics.ScriptActorTurnsCount > turns);
    }

    /// <summary>The operation that a confirmation is recorded under.</summary>
    private static string TagOf(KeyOperation operation) => operation switch
    {
        KeyOperation.Set or KeyOperation.SetMany or KeyOperation.Delete or KeyOperation.DeleteMany or KeyOperation.Extend => "staged_write",
        KeyOperation.PointLock or KeyOperation.ManyPointLocks or KeyOperation.SharedRangeLock or KeyOperation.ExclusiveRangeLock or KeyOperation.LockedProbe => "lock_grant",
        KeyOperation.ReleasePointLock or KeyOperation.ReleaseManyPointLocks or KeyOperation.ReleaseRangeLock => "lock_release",
        _ => "commit_check"
    };

    [Fact]
    public async Task AConfirmationIsRecordedUnderItsOperation_AndAKeptWaitIsCounted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        KeyOperation[] operations = Enum.GetValues<KeyOperation>();
        string[] written = [.. operations.Select(static operation => "wait/written-" + operation)];
        string[] others = [.. operations.Select(static operation => "wait/other-" + operation)];
        await Seed(node, "old", ct, [.. written, .. others]);
        long head = await RevisionOf(node, others[0], ct);

        // Other nodes of this process record into the same instruments, so they can only be asked to have grown.
        using MetricCapture byOperation = new("op", Confirm, UndeliveredKey);
        using MetricCapture byLease = new("lease", Confirm);

        for (int i = 0; i < operations.Length; i++)
        {
            int recorded = byOperation.Samples(Confirm, TagOf(operations[i])).Count;
            Assert.True((await Promptly(() => Perform(node, operations[i], Now(node), others[i], head, ct))).Served, operations[i].ToString());
            Assert.True(byOperation.Samples(Confirm, TagOf(operations[i])).Count > recorded, $"{operations[i]} recorded no confirmation");
        }

        Assert.NotEmpty(byLease.Samples(Confirm, "hit"));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, written[0]), ct);

        foreach (string key in written)
            await Set(node.Kahuna, key, "new", ct);

        List<Task<Answer>> waiting = [];

        for (int i = 0; i < operations.Length; i++)
        {
            double counted = byOperation.Total(UndeliveredKey, TagOf(operations[i]));
            waiting.Add(Perform(node, operations[i], Now(node), written[i], head + 1, ct));
            await WaitUntilAsync(() => byOperation.Total(UndeliveredKey, TagOf(operations[i])) > counted);
        }

        await held.Resume();
        await Task.WhenAll(waiting);
        Assert.NotEmpty(byLease.Samples(Confirm, "miss"));
    }

    [Fact]
    public void EveryOperationHasItsOwnTag() =>
        Assert.Equal(
            Enum.GetValues<KeyValueActorOperation>().Length,
            Enum.GetValues<KeyValueActorOperation>().Select(KeyValueMutationMetrics.OperationName).Distinct().Count());
}

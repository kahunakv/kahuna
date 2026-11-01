using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kommander;
using Kommander.Time;

using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transactional set-many stages every item or none. An item whose condition fails answers <c>NotSet</c>, an item
/// that met a conflict answers its transient type with the holder, and every item that passed answers
/// <c>NotApplied</c>; nothing is staged, a same-transaction read sees no staged value, a commit stores nothing, and a
/// resend of the whole batch stages everything. These tests drive the registered entry point a database consumer
/// uses, on one actor, across the actors of one node, and across the partition leaders of a key-range cluster.
/// </summary>
public sealed class TestAllOrNothingSetMany : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestAllOrNothingSetMany(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static readonly HLCTimestamp HolderTransaction = new(0, 1_000, 0);

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static KahunaSetKeyValueRequestItem Item(TransactionHandle handle, string key, string value, KeyValueFlags flags = KeyValueFlags.None) => new()
    {
        TransactionId = handle.TransactionId,
        Key = key,
        Value = V(value),
        CompareRevision = -1,
        Flags = flags,
        Durability = KeyValueDurability.Persistent
    };

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, string seedKey, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            KeyValueWorkers = 4
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(seedKey, ct);

        return node;
    }

    private static async Task<TransactionHandle> StartTransaction(IKahuna kahuna, string coordinatorKey, KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndStartTransaction(new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = locking,
                Timeout = 30_000
            }, ct),
            static r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    /// <summary>A committed write outside any transaction, retried past the start-up refusals of a fresh node.</summary>
    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, V(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            static r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>
    /// Sends the registered batch until every item has a definite answer: a fresh node can answer an infrastructure
    /// <c>MustRetry</c> that names no holder, which had no effect. A <c>MustRetry</c> that names a holder is a
    /// conflict under test and is returned as is.
    /// </summary>
    private static async Task<List<KahunaSetKeyValueResponseItem>> SetMany(IKahuna kahuna, TransactionHandle handle, TransactionOperationId operationId, List<KahunaSetKeyValueRequestItem> items, CancellationToken ct)
    {
        List<KahunaSetKeyValueResponseItem> responses = await kahuna.LocateAndTrySetManyKeyValue(items, ct, handle.CoordinatorKey, operationId);

        long deadline = Environment.TickCount64 + (long)(10_000 * TimingScale);
        while (Environment.TickCount64 < deadline
               && responses.Exists(static r => r.Type == KeyValueResponseType.MustRetry && r.HolderTransactionId == HLCTimestamp.Zero))
        {
            await Task.Delay(50, ct);
            responses = await kahuna.LocateAndTrySetManyKeyValue(items, ct, handle.CoordinatorKey, operationId);
        }

        return responses;
    }

    private static KahunaSetKeyValueResponseItem ItemFor(List<KahunaSetKeyValueResponseItem> responses, string key) =>
        Assert.Single(responses, r => string.Equals(r.Key, key, StringComparison.Ordinal));

    private static async Task AssertStoredValue(IKahuna kahuna, string key, string? expected, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            static r => r.Item1);

        if (expected is null)
        {
            Assert.True(type == KeyValueResponseType.DoesNotExist || entry?.Value is null, $"{key} should not be stored but answered {type}");
            return;
        }

        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.NotNull(entry);
        Assert.Equal(expected, Encoding.UTF8.GetString(entry!.Value!));
    }

    /// <summary>
    /// Two key spaces of the shape a database writes together (a row record and its index entry), chosen so they are
    /// owned by two different actors of the node, which is what makes a batch over them run its check round and its
    /// stage round instead of one turn.
    /// </summary>
    private static (string Rows, string Index) KeySpacesOnDifferentActors(EmbeddedKahunaNode node, string prefix)
    {
        int actors = ((KahunaManager)node.Kahuna).KeyValues.PersistentInstances.Count;
        Assert.True(actors > 1, "the node must run more than one key-value actor");

        string rows = prefix + "/rows";
        int rowsActor = (int)(HashUtils.InversePrefixedStaticHash(rows + "/x", '/') & int.MaxValue) % actors;

        for (int i = 0; i < 64; i++)
        {
            string index = prefix + "/idx" + i;
            if ((int)(HashUtils.InversePrefixedStaticHash(index + "/x", '/') & int.MaxValue) % actors != rowsActor)
                return (rows, index);
        }

        throw new InvalidOperationException("no candidate key space hashed to another actor");
    }

    // ── one actor ────────────────────────────────────────────────────────────

    /// <summary>
    /// One key space, one actor, one turn: the refused unique key answers <c>NotSet</c>, its siblings
    /// <c>NotApplied</c>; a same-transaction read of a sibling sees no staged value, the working set holds no modified
    /// key, the commit stores nothing, and a later write of a sibling is unaffected.
    /// </summary>
    [Fact]
    public async Task RefusedItem_OneActor_StagesNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/one/seed", ct);
        IKahuna kahuna = node.Kahuna;

        await Seed(kahuna, "aon/one/existing", "orig", ct);

        TransactionHandle handle = await StartTransaction(kahuna, "aon/one/coordinator", KeyValueTransactionLocking.Pessimistic, ct);

        List<KahunaSetKeyValueResponseItem> responses = await SetMany(kahuna, handle, TransactionOperationId.NewRandom(),
        [
            Item(handle, "aon/one/fresh1", "f1"),
            Item(handle, "aon/one/existing", "new", KeyValueFlags.SetIfNotExists),
            Item(handle, "aon/one/fresh2", "f2")
        ], ct);

        Assert.Equal(3, responses.Count);
        Assert.Equal(KeyValueResponseType.NotSet, ItemFor(responses, "aon/one/existing").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(responses, "aon/one/fresh1").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(responses, "aon/one/fresh2").Type);

        // The transaction itself sees no staged value for a sibling.
        (KeyValueResponseType ownRead, ReadOnlyKeyValueEntry? ownEntry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(handle.TransactionId, "aon/one/fresh1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, TransactionOperationId.NewRandom()),
            static r => r.Item1);
        Assert.NotEqual(KeyValueResponseType.Get, ownRead);
        Assert.Null(ownEntry?.Value);

        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
        Assert.NotNull(workingSet);
        Assert.Empty(workingSet!.ModifiedKeys);

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);

        await AssertStoredValue(kahuna, "aon/one/existing", "orig", ct);
        await AssertStoredValue(kahuna, "aon/one/fresh1", null, ct);
        await AssertStoredValue(kahuna, "aon/one/fresh2", null, ct);

        // Nothing of the refused batch lingers on the siblings: a later plain write of one succeeds.
        await Seed(kahuna, "aon/one/fresh1", "later", ct);
        await AssertStoredValue(kahuna, "aon/one/fresh1", "later", ct);
    }

    /// <summary>Every refused key is reported when two items fail their conditions.</summary>
    [Fact]
    public async Task TwoRefusedItems_BothReported()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/two/seed", ct);
        IKahuna kahuna = node.Kahuna;

        await Seed(kahuna, "aon/two/a", "a", ct);
        await Seed(kahuna, "aon/two/b", "b", ct);

        TransactionHandle handle = await StartTransaction(kahuna, "aon/two/coordinator", KeyValueTransactionLocking.Pessimistic, ct);

        List<KahunaSetKeyValueResponseItem> responses = await SetMany(kahuna, handle, TransactionOperationId.NewRandom(),
        [
            Item(handle, "aon/two/a", "a2", KeyValueFlags.SetIfNotExists),
            Item(handle, "aon/two/fresh", "f"),
            Item(handle, "aon/two/b", "b2", KeyValueFlags.SetIfEqualToRevision) // compare revision -1 never matches a stored key
        ], ct);

        Assert.Equal(KeyValueResponseType.NotSet, ItemFor(responses, "aon/two/a").Type);
        Assert.Equal(KeyValueResponseType.NotSet, ItemFor(responses, "aon/two/b").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(responses, "aon/two/fresh").Type);

        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(handle, ct));
        await AssertStoredValue(kahuna, "aon/two/fresh", null, ct);
    }

    /// <summary>
    /// An item that meets a foreign exclusive lock answers <c>MustRetry</c> with the holder and the batch stages
    /// nothing. After the holder releases, the whole-batch resend stages everything and the commit stores it.
    /// </summary>
    [Fact]
    public async Task ForeignLock_RefusesWholeBatch_ThenResendStagesAll()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/lock/seed", ct);
        IKahuna kahuna = node.Kahuna;

        (KeyValueResponseType locked, _, _, _) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryAcquireExclusiveLock(HolderTransaction, "aon/lock/b", 60_000, KeyValueDurability.Persistent, ct),
            static r => r.Item1);
        Assert.Equal(KeyValueResponseType.Locked, locked);

        TransactionHandle handle = await StartTransaction(kahuna, "aon/lock/coordinator", KeyValueTransactionLocking.Optimistic, ct);

        List<KahunaSetKeyValueRequestItem> items =
        [
            Item(handle, "aon/lock/a", "a"),
            Item(handle, "aon/lock/b", "b"),
            Item(handle, "aon/lock/c", "c")
        ];

        List<KahunaSetKeyValueResponseItem> refused = await SetMany(kahuna, handle, TransactionOperationId.NewRandom(), items, ct);

        KahunaSetKeyValueResponseItem blocked = ItemFor(refused, "aon/lock/b");
        Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
        Assert.Equal(HolderTransaction, blocked.HolderTransactionId);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, "aon/lock/a").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, "aon/lock/c").Type);

        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
        Assert.NotNull(workingSet);
        Assert.Empty(workingSet!.ModifiedKeys);

        (KeyValueResponseType released, _) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryReleaseExclusiveLock(HolderTransaction, "aon/lock/b", KeyValueDurability.Persistent, ct),
            static r => r.Item1);
        Assert.Equal(KeyValueResponseType.Unlocked, released);

        List<KahunaSetKeyValueResponseItem> staged = await SetMany(kahuna, handle, TransactionOperationId.NewRandom(), items, ct);
        Assert.All(staged, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);

        await AssertStoredValue(kahuna, "aon/lock/a", "a", ct);
        await AssertStoredValue(kahuna, "aon/lock/b", "b", ct);
        await AssertStoredValue(kahuna, "aon/lock/c", "c", ct);
    }

    /// <summary>
    /// A refused batch leaves no registration behind: the same operation id, resent after the condition clears,
    /// executes again instead of replaying the refusal, and the batch then stages.
    /// </summary>
    [Fact]
    public async Task RefusedBatch_CancelsRegistration_SameIdResendExecutes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/reg/seed", ct);
        IKahuna kahuna = node.Kahuna;

        await Seed(kahuna, "aon/reg/unique", "taken", ct);

        TransactionHandle handle = await StartTransaction(kahuna, "aon/reg/coordinator", KeyValueTransactionLocking.Pessimistic, ct);
        TransactionOperationId operationId = TransactionOperationId.NewRandom();

        List<KahunaSetKeyValueRequestItem> items =
        [
            Item(handle, "aon/reg/row", "row"),
            Item(handle, "aon/reg/unique", "mine", KeyValueFlags.SetIfNotExists)
        ];

        List<KahunaSetKeyValueResponseItem> refused = await SetMany(kahuna, handle, operationId, items, ct);
        Assert.Equal(KeyValueResponseType.NotSet, ItemFor(refused, "aon/reg/unique").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, "aon/reg/row").Type);

        // The condition clears outside the transaction; the identical batch under the identical id must run again.
        (KeyValueResponseType deleted, _, _) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, "aon/reg/unique", KeyValueDurability.Persistent, ct),
            static r => r.Item1);
        Assert.Equal(KeyValueResponseType.Deleted, deleted);

        List<KahunaSetKeyValueResponseItem> staged = await SetMany(kahuna, handle, operationId, items, ct);
        Assert.All(staged, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);

        await AssertStoredValue(kahuna, "aon/reg/row", "row", ct);
        await AssertStoredValue(kahuna, "aon/reg/unique", "mine", ct);
    }

    /// <summary>A transactional batch that names one key twice, or mixes transactions, is refused whole.</summary>
    [Fact]
    public async Task DuplicateKeyOrMixedTransactions_IsInvalidInput()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/shape/seed", ct);
        IKahuna kahuna = node.Kahuna;

        TransactionHandle handle = await StartTransaction(kahuna, "aon/shape/coordinator", KeyValueTransactionLocking.Pessimistic, ct);

        List<KahunaSetKeyValueResponseItem> twice = await kahuna.LocateAndTrySetManyKeyValue(
            [Item(handle, "aon/shape/k", "1"), Item(handle, "aon/shape/k", "2")], ct, handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(2, twice.Count);
        Assert.All(twice, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

        KahunaSetKeyValueRequestItem plain = Item(handle, "aon/shape/plain", "p");
        plain.TransactionId = HLCTimestamp.Zero;

        List<KahunaSetKeyValueResponseItem> mixed = await kahuna.LocateAndTrySetManyKeyValue(
            [Item(handle, "aon/shape/a", "a"), plain], ct, handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(2, mixed.Count);
        Assert.All(mixed, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
        Assert.NotNull(workingSet);
        Assert.Empty(workingSet!.ModifiedKeys);

        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(handle, ct));
    }

    // ── several actors of one node ───────────────────────────────────────────

    /// <summary>
    /// The shape of a database INSERT of two rows: each row's record and its primary-key index entry go in one batch,
    /// and the two key spaces are owned by two actors. Row 1 already exists, so its index entry is refused. Nothing
    /// is staged on either actor, the working set is empty, and the batch without the duplicate stages everything.
    /// </summary>
    [Fact]
    public async Task RefusedItem_TwoActors_StagesNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/multi/seed", ct);
        IKahuna kahuna = node.Kahuna;

        (string rows, string index) = KeySpacesOnDifferentActors(node, "aon/multi");

        await Seed(kahuna, rows + "/1", "row1", ct);
        await Seed(kahuna, index + "/pk/1", rows + "/1", ct);

        TransactionHandle handle = await StartTransaction(kahuna, "aon/multi/coordinator", KeyValueTransactionLocking.Pessimistic, ct);

        List<KahunaSetKeyValueResponseItem> refused = await SetMany(kahuna, handle, TransactionOperationId.NewRandom(),
        [
            Item(handle, rows + "/21", "row21"),
            Item(handle, index + "/pk/21", rows + "/21", KeyValueFlags.SetIfNotExists),
            Item(handle, rows + "/1b", "row1-twin"),
            Item(handle, index + "/pk/1", rows + "/1b", KeyValueFlags.SetIfNotExists)
        ], ct);

        Assert.Equal(4, refused.Count);
        Assert.Equal(KeyValueResponseType.NotSet, ItemFor(refused, index + "/pk/1").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, rows + "/21").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, index + "/pk/21").Type);
        Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, rows + "/1b").Type);

        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
        Assert.NotNull(workingSet);
        Assert.Empty(workingSet!.ModifiedKeys);

        // The transaction stays usable: the statement without the duplicate row stages on both actors.
        List<KahunaSetKeyValueResponseItem> staged = await SetMany(kahuna, handle, TransactionOperationId.NewRandom(),
        [
            Item(handle, rows + "/21", "row21"),
            Item(handle, index + "/pk/21", rows + "/21", KeyValueFlags.SetIfNotExists)
        ], ct);
        Assert.All(staged, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);

        await AssertStoredValue(kahuna, rows + "/21", "row21", ct);
        await AssertStoredValue(kahuna, index + "/pk/21", rows + "/21", ct);
        await AssertStoredValue(kahuna, rows + "/1b", null, ct);
        await AssertStoredValue(kahuna, index + "/pk/1", rows + "/1", ct);
    }

    /// <summary>
    /// The one documented residual: under optimistic locking a foreign lock that lands between the check round and
    /// the stage round refuses one actor's items after the other actor staged its own. The answers show it (the
    /// staged items <c>Set</c>, the blocked item <c>MustRetry</c> with its holder, its sibling <c>NotApplied</c>),
    /// the working set holds exactly the staged items, and the caller rolls back.
    /// </summary>
    [Fact]
    public async Task OptimisticForeignWriteBetweenRounds_ReportsPartialStaging()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/race/seed", ct);
        IKahuna kahuna = node.Kahuna;
        KeyValuesManager keyValues = ((KahunaManager)kahuna).KeyValues;

        (string rows, string index) = KeySpacesOnDifferentActors(node, "aon/race");

        TransactionHandle handle = await StartTransaction(kahuna, "aon/race/coordinator", KeyValueTransactionLocking.Optimistic, ct);

        int fired = 0;
        keyValues.BeforeSetManyStageRoundForTesting = async transactionId =>
        {
            if (transactionId != handle.TransactionId || Interlocked.Exchange(ref fired, 1) != 0)
                return;

            (KeyValueResponseType locked, _, _, _) = await RetryOnMustRetryAsync(
                () => kahuna.LocateAndTryAcquireExclusiveLock(HolderTransaction, index + "/pk/7", 60_000, KeyValueDurability.Persistent, ct),
                static r => r.Item1);
            Assert.Equal(KeyValueResponseType.Locked, locked);
        };

        try
        {
            List<KahunaSetKeyValueResponseItem> responses = await SetMany(kahuna, handle, TransactionOperationId.NewRandom(),
            [
                Item(handle, rows + "/7", "row7"),
                Item(handle, rows + "/8", "row8"),
                Item(handle, index + "/pk/7", rows + "/7", KeyValueFlags.SetIfNotExists),
                Item(handle, index + "/pk/8", rows + "/8", KeyValueFlags.SetIfNotExists)
            ], ct);

            Assert.Equal(1, fired);
            Assert.Equal(KeyValueResponseType.Set, ItemFor(responses, rows + "/7").Type);
            Assert.Equal(KeyValueResponseType.Set, ItemFor(responses, rows + "/8").Type);

            KahunaSetKeyValueResponseItem blocked = ItemFor(responses, index + "/pk/7");
            Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
            Assert.Equal(HolderTransaction, blocked.HolderTransactionId);
            Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(responses, index + "/pk/8").Type);

            TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(workingSet);
            Assert.Equal(2, workingSet!.ModifiedKeys.Count);
            Assert.Contains(workingSet.ModifiedKeys, m => m.Key == rows + "/7");
            Assert.Contains(workingSet.ModifiedKeys, m => m.Key == rows + "/8");

            Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(handle, ct));
            await AssertStoredValue(kahuna, rows + "/7", null, ct);
            await AssertStoredValue(kahuna, rows + "/8", null, ct);
        }
        finally
        {
            keyValues.BeforeSetManyStageRoundForTesting = null;
        }
    }

    /// <summary>A non-transactional batch is independent writes: a refused conditional item does not touch its siblings.</summary>
    [Fact]
    public async Task NonTransactionalBatch_KeepsIndependentItems()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "aon/plain/seed", ct);
        IKahuna kahuna = node.Kahuna;

        await Seed(kahuna, "aon/plain/existing", "orig", ct);

        List<KahunaSetKeyValueResponseItem> responses = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTrySetManyKeyValue(
            [
                new() { Key = "aon/plain/existing", Value = V("new"), Flags = KeyValueFlags.SetIfNotExists, Durability = KeyValueDurability.Persistent },
                new() { Key = "aon/plain/fresh", Value = V("f"), Flags = KeyValueFlags.Set, Durability = KeyValueDurability.Persistent }
            ], ct),
            static r => r.Exists(static i => i.Type == KeyValueResponseType.MustRetry) ? KeyValueResponseType.MustRetry : KeyValueResponseType.Set);

        Assert.Equal(KeyValueResponseType.NotSet, ItemFor(responses, "aon/plain/existing").Type);
        Assert.Equal(KeyValueResponseType.Set, ItemFor(responses, "aon/plain/fresh").Type);

        await AssertStoredValue(kahuna, "aon/plain/existing", "orig", ct);
        await AssertStoredValue(kahuna, "aon/plain/fresh", "f", ct);
    }

    // ── several partition leaders, key-range sharding ───────────────────────

    private const string RangedSpace = "aon:r";

    /// <summary>
    /// Under key-range sharding one key space spans two partitions led by two different nodes. A batch over both
    /// ranges with one refused unique key stages nothing on either leader (the check round refused before any stage
    /// round ran), and the batch without the refused key stages on both and commits.
    /// </summary>
    [Fact]
    public async Task KeyRange_RefusedItem_StagesNothingOnAnyPartition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 6, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes = [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(RangedSpace);

            (int low, int high) = await TwoPartitionsWithDifferentLeaders(nodes, ct);

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);

            string split = RangedSpace + "/m";
            bool committed = await metaLeader.RangeMapStore.MutateAsync(
                _ =>
                [
                    new RangeDescriptor { KeySpace = RangedSpace, StartKey = null, EndKey = split, PartitionId = low, Generation = 1 },
                    new RangeDescriptor { KeySpace = RangedSpace, StartKey = split, EndKey = null, PartitionId = high, Generation = 1 }
                ],
                ct);
            Assert.True(committed);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(RangedSpace, RangedSpace + "/a")?.PartitionId == low);
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(RangedSpace, RangedSpace + "/z")?.PartitionId == high);
            }

            await Seed(k1, RangedSpace + "/z1", "taken", ct);

            TransactionHandle handle = await StartTransaction(k1, "aon/range/coordinator", KeyValueTransactionLocking.Pessimistic, ct);

            List<KahunaSetKeyValueResponseItem> refused = await SetMany(k1, handle, TransactionOperationId.NewRandom(),
            [
                Item(handle, RangedSpace + "/a1", "a1"),
                Item(handle, RangedSpace + "/z1", "mine", KeyValueFlags.SetIfNotExists),
                Item(handle, RangedSpace + "/z2", "z2")
            ], ct);

            Assert.Equal(3, refused.Count);
            Assert.Equal(KeyValueResponseType.NotSet, ItemFor(refused, RangedSpace + "/z1").Type);
            Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, RangedSpace + "/a1").Type);
            Assert.Equal(KeyValueResponseType.NotApplied, ItemFor(refused, RangedSpace + "/z2").Type);

            TransactionWorkingSet? workingSet = await k1.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(workingSet);
            Assert.Empty(workingSet!.ModifiedKeys);

            List<KahunaSetKeyValueResponseItem> staged = await SetMany(k1, handle, TransactionOperationId.NewRandom(),
            [
                Item(handle, RangedSpace + "/a1", "a1"),
                Item(handle, RangedSpace + "/z2", "z2")
            ], ct);
            Assert.All(staged, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

            (KeyValueResponseType commit, _) = await k1.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commit);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                await AssertStoredValue(kahuna, RangedSpace + "/a1", "a1", ct);
                await AssertStoredValue(kahuna, RangedSpace + "/z2", "z2", ct);
                await AssertStoredValue(kahuna, RangedSpace + "/z1", "taken", ct);
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(partition, ct))
                    return (raft, kahuna);

            await Task.Delay(50, ct);
        }
    }

    /// <summary>Two data partitions led by two different nodes, so a batch over both crosses the inter-node hop.</summary>
    private static async Task<(int Low, int High)> TwoPartitionsWithDifferentLeaders((IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + (long)(30_000 * TimingScale);

        while (Environment.TickCount64 < deadline)
        {
            Dictionary<int, int> leaderByPartition = [];

            for (int partition = 1; partition <= 5; partition++)
            {
                for (int n = 0; n < nodes.Length; n++)
                {
                    if (await nodes[n].Item1.AmILeader(partition, ct))
                    {
                        leaderByPartition[partition] = n;
                        break;
                    }
                }
            }

            foreach ((int first, int firstLeader) in leaderByPartition)
                foreach ((int second, int secondLeader) in leaderByPartition)
                    if (first < second && firstLeader != secondLeader)
                        return (first, second);

            await Task.Delay(100, ct);
        }

        throw new TimeoutException("no two data partitions with different leaders");
    }
}

using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// One embedded node whose ordered apply a test can hold, so that a committed entry stays undelivered for as long as
/// the test needs.
/// </summary>
public abstract class KeyScopedApplyWaitTestBase : BaseCluster
{
    /// <summary>How long an operation must stay unanswered before a test says that it waits.</summary>
    private protected static readonly TimeSpan Pending = TimeSpan.FromMilliseconds(300);

    private protected readonly ILoggerFactory loggerFactory;

    private protected KeyScopedApplyWaitTestBase(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    /// <summary>
    /// Runs an operation until one attempt answers without a wait. An attempt that meets a lapsed lease runs a round,
    /// which waits for the delivery with it, so one attempt that waits proves nothing.
    /// </summary>
    private protected static async Task<T> Promptly<T>(Func<Task<T>> operation)
    {
        for (int attempt = 0; attempt < 40; attempt++)
        {
            Task<T> task = operation();

            if (await Task.WhenAny(task, Task.Delay(250, TestContext.Current.CancellationToken)) == task)
                return await task;
        }

        throw new TimeoutException("no attempt of the operation answered");
    }

    private protected async Task<EmbeddedKahunaNode> StartNode(
        CancellationToken ct, Action<EmbeddedKahunaOptions>? configure = null, int partitions = 1)
    {
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = partitions
        };
        configure?.Invoke(options);

        EmbeddedKahunaNode node = new(options, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("wait0/k0", ct);

        return node;
    }

    private protected static KahunaManager ManagerOf(EmbeddedKahunaNode node) => (KahunaManager)node.Kahuna;

    private protected static int PartitionOf(EmbeddedKahunaNode node, string key) => ManagerOf(node).KeyValues.LocateDurablePartition(key).PartitionId;

    private protected static bool Marked(EmbeddedKahunaNode node, string key) => ManagerOf(node).KeyValues.UndeliveredKeys.MayChange(PartitionOf(node, key), key);

    private protected static HLCTimestamp Now(EmbeddedKahunaNode node) => node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

    /// <summary>Holds the delivery of the committed entries of one partition until disposed.</summary>
    private protected sealed class HeldDelivery(IRaft raft, int partition) : IAsyncDisposable
    {
        private bool resumed;

        public static async Task<HeldDelivery> Start(IRaft raft, int partition, CancellationToken ct)
        {
            Assert.Equal(RaftOperationStatus.Success, await raft.HoldConsumerAppliesForTesting(partition, ct));
            return new HeldDelivery(raft, partition);
        }

        public async Task Resume()
        {
            if (resumed)
                return;

            resumed = true;
            Assert.Equal(RaftOperationStatus.Success, await raft.ResumeConsumerAppliesForTesting(partition, CancellationToken.None));
        }

        public async ValueTask DisposeAsync() => await Resume();
    }

    private protected static async Task Set(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>Sets the keys and waits until nothing of them is undelivered.</summary>
    private protected static async Task Seed(EmbeddedKahunaNode node, string value, CancellationToken ct, params string[] keys)
    {
        foreach (string key in keys)
            await Set(node.Kahuna, key, value, ct);

        await WaitUntilAsync(() => !keys.Any(key => Marked(node, key)));
    }

    private protected static async Task<string?> Get(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        return type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null;
    }

    private protected static async Task<TransactionHandle> Start(
        IKahuna kahuna, string coordinatorKey, CancellationToken ct, KeyValueTransactionLocking locking = KeyValueTransactionLocking.Optimistic)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = locking,
                ReadValidation = ReadValidation.TrackAndValidate,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private protected static async Task Stage(
        IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0, durability, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, type);
    }
}

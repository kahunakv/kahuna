
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// How a batched read (<c>TryGetManyValues</c>) gathers its answers. A key the published head record answers is
/// answered while the batch starts, on the calling thread; a key that needs its actor is asked then and awaited
/// afterwards, by one state machine for the whole batch. The result must still be one answer per key in input
/// order, every started read must be awaited even when one of them fails, and a batch whose keys several nodes
/// lead must still answer every key.
/// </summary>
public sealed class TestBatchedReadFanIn
{
    private readonly ILoggerFactory loggerFactory;

    public TestBatchedReadFanIn(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// A batch that mixes keys answered from their published head record with keys that go to their actor (keys
    /// that do not exist have no record to answer from) returns one answer per key, in the order of the request.
    /// </summary>
    [Fact]
    public async Task MixOfPublishedHeadAnswersAndActorReads_ReturnsOneAnswerPerKeyInInputOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await TestScanObservations.StartNode(loggerFactory, "memory", ct);
        IKahuna kahuna = node.Kahuna;

        for (int i = 0; i < 6; i++)
        {
            (KeyValueResponseType set, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"fan/k{i}", Encoding.UTF8.GetBytes("value-" + i), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, set);

            // One read through the actor publishes the head record the batch can then answer from.
            (KeyValueResponseType got, _) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"fan/k{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, got);
        }

        string[] order = ["fan/k3", "fan/absent-a", "fan/k0", "fan/absent-b", "fan/k5", "fan/k1", "fan/absent-c", "fan/k4", "fan/k2"];

        List<(string key, long revision, KeyValueDurability durability)> keys =
            order.Select(static key => (key, -1L, KeyValueDurability.Persistent)).ToList();

        for (int attempt = 0; attempt < 3; attempt++)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> answers =
                await kahuna.LocateAndTryGetManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, keys, ct);

            Assert.Equal(order, answers.Select(static answer => answer.key).ToArray());

            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in answers)
            {
                Assert.Equal(KeyValueDurability.Persistent, durability);

                if (key.StartsWith("fan/absent", StringComparison.Ordinal))
                {
                    Assert.Equal(KeyValueResponseType.DoesNotExist, type);
                    continue;
                }

                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.Equal("value-" + key[^1], Encoding.UTF8.GetString(entry!.Value!));
            }
        }
    }

    /// <summary>
    /// The reads left in flight after the starting pass each fill the slot of their own key, whatever order they
    /// complete in, and the batch completes only when the last of them has.
    /// </summary>
    [Fact]
    public async Task PendingReads_FillTheirOwnSlots_AndTheBatchWaitsForTheLastOne()
    {
        TaskCompletionSource<LocalPointRead> first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<LocalPointRead> second = new(TaskCreationOptions.RunContinuationsAsynchronously);

        ReadOnlyKeyValueEntry answered = Entry(7);
        ReadOnlyKeyValueEntry late = Entry(8);
        ReadOnlyKeyValueEntry later = Entry(9);

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results =
        [
            (KeyValueResponseType.Errored, "k/a", KeyValueDurability.Persistent, null),
            (KeyValueResponseType.Get, "k/b", KeyValueDurability.Persistent, answered),
            (KeyValueResponseType.Errored, "k/c", KeyValueDurability.Ephemeral, null)
        ];

        Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> batch =
            LocalKeyValueReadOperations.AwaitPendingReads(
                results, [(0, new ValueTask<LocalPointRead>(first.Task)), (2, new ValueTask<LocalPointRead>(second.Task))]);

        // The later key answers first; the batch still waits for the earlier one.
        second.SetResult(new LocalPointRead(KeyValueResponseType.Get, later, false));
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(batch.IsCompleted);

        first.SetResult(new LocalPointRead(KeyValueResponseType.Get, late, false));

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> done = await batch;

        Assert.Same(results, done);
        Assert.Equal((KeyValueResponseType.Get, "k/a", KeyValueDurability.Persistent, late), done[0]);
        Assert.Equal((KeyValueResponseType.Get, "k/b", KeyValueDurability.Persistent, answered), done[1]);
        Assert.Equal((KeyValueResponseType.Get, "k/c", KeyValueDurability.Ephemeral, later), done[2]);
    }

    /// <summary>
    /// A read that fails does not end the batch early: the reads still in flight are awaited first, so none of
    /// them is left unobserved, and then the batch fails with the first failure in input order.
    /// </summary>
    [Fact]
    public async Task AFailedRead_IsThrownOnlyAfterEveryOtherReadHasFinished()
    {
        TaskCompletionSource<LocalPointRead> stillRunning = new(TaskCreationOptions.RunContinuationsAsynchronously);

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results =
        [
            (KeyValueResponseType.Errored, "k/a", KeyValueDurability.Persistent, null),
            (KeyValueResponseType.Errored, "k/b", KeyValueDurability.Persistent, null),
            (KeyValueResponseType.Errored, "k/c", KeyValueDurability.Persistent, null)
        ];

        Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> batch =
            LocalKeyValueReadOperations.AwaitPendingReads(
                results,
                [
                    (0, new ValueTask<LocalPointRead>(Task.FromException<LocalPointRead>(new InvalidOperationException("first failure")))),
                    (1, new ValueTask<LocalPointRead>(Task.FromException<LocalPointRead>(new TimeoutException("second failure")))),
                    (2, new ValueTask<LocalPointRead>(stillRunning.Task))
                ]);

        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(batch.IsCompleted, "the batch must wait for the read that is still running");

        ReadOnlyKeyValueEntry answered = Entry(3);
        stillRunning.SetResult(new LocalPointRead(KeyValueResponseType.Get, answered, false));

        InvalidOperationException failure = await Assert.ThrowsAsync<InvalidOperationException>(() => batch);
        Assert.Equal("first failure", failure.Message);

        // The read that finished was still recorded before the failure was reported.
        Assert.Same(answered, results[2].Item4);
    }

    private static ReadOnlyKeyValueEntry Entry(long revision) =>
        new(Encoding.UTF8.GetBytes("v" + revision), revision, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 1_000 + revision, 0), KeyValueState.Set);
}

/// <summary>
/// A batched read on a cluster. The keys belong to many key spaces, so their partitions are led by several
/// nodes: the node that receives the batch reads the keys it leads itself and sends the others to their leaders.
/// </summary>
public sealed class TestBatchedReadAcrossLeaders : BaseCluster
{
    private readonly ITestOutputHelper output;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestBatchedReadAcrossLeaders(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// Whichever node receives the batch, every key is answered exactly once with the value that was written,
    /// both when all the keys of the batch have one leader and when they have several. A batch of one key space
    /// has one leader; the batch of all key spaces spans every leader the cluster has.
    /// </summary>
    [Fact]
    public async Task EveryKeyIsAnsweredOnce_WhicheverNodeReceivesTheBatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int partitions = 8;
        const int keySpaces = 48;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", partitions, raftLogger, kahunaLogger);

        try
        {
            string run = Guid.NewGuid().ToString("N")[..8];
            Dictionary<string, string> written = new(StringComparer.Ordinal);

            for (int space = 0; space < keySpaces; space++)
            {
                for (int item = 0; item < 2; item++)
                {
                    string key = $"span-{run}-{space:D2}/k{item}";
                    string value = $"value-{space}-{item}";

                    (KeyValueResponseType set, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                    Assert.Equal(KeyValueResponseType.Set, set);

                    written[key] = value;
                }
            }

            HashSet<string> leaders = new(StringComparer.Ordinal);
            for (int partition = 1; partition <= partitions; partition++)
                leaders.Add(await node1.WaitForLeader(partition, ct));

            output.WriteLine($"{partitions} partitions are led by {leaders.Count} node(s)");

            List<(string key, long revision, KeyValueDurability durability)> everyKey =
                written.Keys.Select(static key => (key, -1L, KeyValueDurability.Persistent)).ToList();

            List<(string key, long revision, KeyValueDurability durability)> oneKeySpace =
                everyKey.Where(entry => entry.key.StartsWith($"span-{run}-07/", StringComparison.Ordinal)).ToList();

            foreach (IKahuna receiver in new[] { kahuna1, kahuna2, kahuna3 })
            {
                await AssertBatchAnswersEveryKey(receiver, everyKey, written, ct);
                await AssertBatchAnswersEveryKey(receiver, oneKeySpace, written, ct);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    private static async Task AssertBatchAnswersEveryKey(
        IKahuna receiver,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Dictionary<string, string> written,
        CancellationToken ct)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> answers = [];

        // A leadership confirmation can answer MustRetry while the cluster settles; the batch is then asked again.
        for (int attempt = 0; attempt < 50; attempt++)
        {
            answers = await receiver.LocateAndTryGetManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, keys, ct);

            if (answers.All(static answer => answer.type != KeyValueResponseType.MustRetry))
                break;

            await Task.Delay(50, ct);
        }

        Assert.Equal(keys.Count, answers.Count);
        Assert.Equal(
            keys.Select(static entry => entry.key).OrderBy(static key => key, StringComparer.Ordinal),
            answers.Select(static answer => answer.key).OrderBy(static key => key, StringComparer.Ordinal));

        foreach ((KeyValueResponseType type, string key, KeyValueDurability _, ReadOnlyKeyValueEntry? entry) in answers)
        {
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(written[key], Encoding.UTF8.GetString(entry!.Value!));
        }
    }
}

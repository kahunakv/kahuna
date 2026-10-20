using System.Collections.Concurrent;
using System.Text;
using Kahuna.Client;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a change-feed consumer needs from a commit: the origin label a transaction names at start reaches every
/// prepared intent and every watch event of the transaction (only on a cluster that enables origin tags), and the
/// commit answers the transaction's commit timestamp, the same one its watch events report.
/// </summary>
public sealed class TestTransactionOrigin : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestTransactionOrigin(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Fact]
    public async Task Origin_ReachesTheWatchEvent_AndTheCommitAnswersItsTimestamp()
    {
        await WithCluster(tags: true, async (nodes, ct) =>
        {
            string group = $"orig{Guid.NewGuid():N}|";
            await using EventCollector events = await Watch(nodes[1], group, ct);

            (TransactionHandle tagged, TransactionCommitAnswer taggedAnswer) = await Commit(nodes[0], group + "a", "schema-rewrite", ct);
            (TransactionHandle plain, TransactionCommitAnswer plainAnswer) = await Commit(nodes[2], group + "b", null, ct);

            Assert.NotEqual(HLCTimestamp.Zero, taggedAnswer.CommitTimestamp);
            Assert.NotEqual(HLCTimestamp.Zero, plainAnswer.CommitTimestamp);

            KeyValueWatchEvent a = await events.WaitFor(group + "a");
            KeyValueWatchEvent b = await events.WaitFor(group + "b");

            Assert.Equal("schema-rewrite", a.Origin);
            Assert.Equal(tagged.TransactionId, a.TransactionId);
            Assert.Equal(taggedAnswer.CommitTimestamp, a.CommitTimestamp);

            Assert.Null(b.Origin);
            Assert.Equal(plain.TransactionId, b.TransactionId);
            Assert.Equal(plainAnswer.CommitTimestamp, b.CommitTimestamp);
        });
    }

    [Fact]
    public async Task Origin_IsDropped_WhileTheClusterDoesNotEnableIt()
    {
        await WithCluster(tags: false, async (nodes, ct) =>
        {
            string group = $"notag{Guid.NewGuid():N}|";
            await using EventCollector events = await Watch(nodes[0], group, ct);

            await Commit(nodes[1], group + "a", "schema-rewrite", ct);

            Assert.Null((await events.WaitFor(group + "a")).Origin);
        });
    }

    [Fact]
    public async Task Origin_LongerThanTheBound_IsRefusedAtStart()
    {
        await WithCluster(tags: true, async (nodes, ct) =>
        {
            (KeyValueResponseType type, _) = await nodes[0].LocateAndStartTransaction(new KeyValueTransactionOptions
            {
                CoordinatorKey = "orig-long",
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 10_000,
                Origin = new string('x', TransactionCoordinator.MaxOriginLength + 1)
            }, ct);

            Assert.Equal(KeyValueResponseType.InvalidInput, type);
        });
    }

    [Fact]
    public async Task ClientSession_CommitWithTimestamp_AnswersTheTimestampTheEventCarries()
    {
        await WithCluster(tags: true, async (nodes, ct) =>
        {
            string group = $"client{Guid.NewGuid():N}|";
            await using EventCollector events = await Watch(nodes[2], group, ct);

            Dictionary<string, IKahuna> byUrl = new() { ["node-0"] = nodes[0], ["node-1"] = nodes[1], ["node-2"] = nodes[2] };
            KahunaClient client = new(["node-0", "node-1", "node-2"], communication: new InProcessKahunaCommunication(byUrl));

            (bool committed, HLCTimestamp commitTimestamp) = (false, HLCTimestamp.Zero);
            for (int attempt = 0; attempt < 10 && !committed; attempt++)
            {
                await using KahunaTransactionSession session = await client.StartTransactionSession(
                    new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 10_000, Origin = "ttl-sweep" }, ct);

                await session.SetKeyValue(group + "k", "v"u8.ToArray(), cancellationToken: ct);
                (committed, commitTimestamp) = await session.CommitWithTimestamp(ct);
            }

            Assert.True(committed);
            KeyValueWatchEvent e = await events.WaitFor(group + "k");
            Assert.Equal(commitTimestamp, e.CommitTimestamp);
            Assert.Equal("ttl-sweep", e.Origin);
        });
    }

    [Fact]
    public void Origin_SurvivesTheDeltaCodec_AndJoinsTheDigest()
    {
        PreparedIntent plain = new(
            new HLCTimestamp(1, 10, 0), 1, "orig/k", 0, "orig/k", new HLCTimestamp(1, 11, 0),
            KeyValueState.Set, [1], null, 1, HLCTimestamp.Zero, false, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined,
            HLCTimestamp.Zero, PreparedIntentResolution.Pending);
        PreparedIntent tagged = plain with { Origin = "rewrite" };

        byte[] delta = PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(tagged), new PrepareIntentCommand(plain with { Key = "orig/j" })]);

        PreparedIntentCommand[] decoded = PreparedIntentStore.DecodeDelta([.. delta]);
        Assert.Equal("rewrite", Assert.IsType<PrepareIntentCommand>(decoded[0]).Intent.Origin);
        Assert.Null(Assert.IsType<PrepareIntentCommand>(decoded[1]).Intent.Origin);

        PreparedIntentDeltaMessage message = PreparedIntentDeltaMessage.Parser.ParseFrom(delta);
        Assert.Equal("rewrite", Assert.IsType<PrepareIntentCommand>(PreparedIntentStore.ToCommand(message.Commands[0], message.Header)).Intent.Origin);

        // A re-prepare with another origin is another mutation; an intent with no origin keeps the digest an older
        // node computes.
        Assert.False(PreparedIntentDigest.Matches(plain, tagged));
        Assert.False(PreparedIntentDigest.Matches(tagged, tagged with { Origin = "drop" }));
        Assert.True(PreparedIntentDigest.Matches(plain, plain with { Resolution = PreparedIntentResolution.Committed }));
    }

    [Fact]
    public void WatchWire_CarriesTheOrigin_OnBothShapes()
    {
        KeyValueWatchFrame frame = KeyValueWatchFrame.ForEvent(new KeyValueWatchEvent(
            "orig/k", KeyValueWatchEventKind.Set, [7], 3, HLCTimestamp.Zero, new HLCTimestamp(1, 5, 0), new HLCTimestamp(1, 4, 0), 2, "rewrite"));

        Assert.Equal("rewrite", KeyValueWatchWire.FromGrpc(KeyValueWatchWire.ToGrpc(1, frame)).Event!.Origin);
        Assert.Equal("rewrite", KeyValueWatchWire.FromRest(KeyValueWatchWire.ToRest(frame)).Event!.Origin);

        KeyValueWatchFrame untagged = KeyValueWatchFrame.ForEvent(new KeyValueWatchEvent(
            "orig/k", KeyValueWatchEventKind.Set, [7], 3, HLCTimestamp.Zero, new HLCTimestamp(1, 5, 0), HLCTimestamp.Zero, 2));

        Assert.Null(KeyValueWatchWire.FromGrpc(KeyValueWatchWire.ToGrpc(1, untagged)).Event!.Origin);
        Assert.Null(KeyValueWatchWire.FromRest(KeyValueWatchWire.ToRest(untagged)).Event!.Origin);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private async Task WithCluster(bool tags, Func<KahunaManager[], CancellationToken, Task> body)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, c => c.TransactionOriginTags = tags);

        try
        {
            await body([(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3], ct);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // Runs one interactive transaction that writes one key, retrying a retryable answer.
    private static async Task<(TransactionHandle Handle, TransactionCommitAnswer Answer)> Commit(KahunaManager node, string key, string? origin, CancellationToken ct)
    {
        for (int attempt = 0; ; attempt++)
        {
            (KeyValueResponseType started, TransactionHandle handle) = await node.LocateAndStartTransaction(new KeyValueTransactionOptions
            {
                CoordinatorKey = key,
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 10_000,
                Origin = origin
            }, ct);
            Assert.Equal(KeyValueResponseType.Set, started);

            (KeyValueResponseType set, _, _) = await node.LocateAndTrySetKeyValue(
                handle.TransactionId, key, Encoding.UTF8.GetBytes("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            if (set == KeyValueResponseType.Set)
            {
                TransactionCommitAnswer answer = await node.LocateAndCommitTransaction(handle, ct);
                if (answer.Type == KeyValueResponseType.Committed)
                    return (handle, answer);
            }

            await node.LocateAndRollbackTransaction(handle, ct);

            Assert.True(attempt < 10, "the transaction kept failing");
            await Task.Delay(100, ct);
        }
    }

    private static async Task<EventCollector> Watch(KahunaManager node, string group, CancellationToken ct)
    {
        KeyValueWatchOpening opening = await RetryOnMustRetryAsync(() => node.WatchKeyValues(new(KeyValueWatchScope.ForPrefix(group)), ct), o => o.Type);
        Assert.Equal(KeyValueResponseType.Get, opening.Type);
        return new EventCollector(opening.Watch!);
    }

    private sealed class EventCollector : IAsyncDisposable
    {
        private readonly ConcurrentQueue<KeyValueWatchEvent> events = new();

        private readonly CancellationTokenSource stop = new();

        private readonly KeyValueWatch watch;

        private readonly Task pump;

        public EventCollector(KeyValueWatch watch)
        {
            this.watch = watch;
            pump = Task.Run(async () =>
            {
                try
                {
                    await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(stop.Token))
                    {
                        if (frame.Event is { } e)
                            events.Enqueue(e);
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });
        }

        public async Task<KeyValueWatchEvent> WaitFor(string key)
        {
            await WaitUntilAsync(() => events.Any(e => e.Key == key));
            return events.First(e => e.Key == key);
        }

        public async ValueTask DisposeAsync()
        {
            await watch.DisposeAsync();
            await stop.CancelAsync();
            await pump;
            stop.Dispose();
        }
    }
}

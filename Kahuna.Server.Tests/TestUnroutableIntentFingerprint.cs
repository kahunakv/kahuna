using System.Runtime.ExceptionServices;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The per-partition apply fingerprint counts the prepared intents whose key routes to the partition, and it is
/// read on every replica several times at every leader change. A node can hold intents it cannot route: the key
/// belongs to a key-range space with no descriptor covering it on that node (a restart replays data-partition
/// entries before the meta partition has rebuilt the range map). The count used to learn that from an exception
/// per intent, so a replica holding a few thousand such intents threw that many times per read: under a run of
/// leader changes the exceptions took whole cores and hundreds of megabytes per second of stack-trace buffers.
/// An unroutable intent must cost a lookup.
/// </summary>
public sealed class TestUnroutableIntentFingerprint : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int Partition = 1;

    private const int UnroutableIntents = 64;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestUnroutableIntentFingerprint(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static PreparedIntent MakeIntent(HLCTimestamp txId, string space, string key) => new(
        TransactionId: txId, Epoch: 1, Key: key, ManifestHash: 42, RecordAnchorKey: key,
        CommitTimestamp: txId, State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes("v"), Bucket: space,
        Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: 0, BaseState: KeyValueState.Set,
        RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
        Resolution: PreparedIntentResolution.Pending);

    [Fact]
    public async Task FingerprintRead_OverIntentsNoDescriptorCovers_RaisesNoException_AndLeavesThemOut()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        // Unique per run: the exception probe below is process-wide, and the space name is what ties a routing
        // failure to this cluster.
        string space = $"ur{Guid.NewGuid():N}:s";

        long routingFailures = 0;
        EventHandler<FirstChanceExceptionEventArgs> onFirstChance = (_, e) =>
        {
            if (e.Exception is KahunaServerException && e.Exception.Message.Contains(space, StringComparison.Ordinal))
                Interlocked.Increment(ref routingFailures);
        };

        try
        {
            // The space routes by key range on every node, and no node holds a descriptor for it.
            foreach (KahunaManager manager in managers)
                manager.RegisterKeyRange(space);

            List<PreparedIntent> intents = new(UnroutableIntents);
            for (int i = 0; i < UnroutableIntents; i++)
            {
                HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
                intents.Add(MakeIntent(txId, space, $"{space}/k{i:D4}"));
            }

            AppDomain.CurrentDomain.FirstChanceException += onFirstChance;

            // The import replicates through the partition, so every replica applies the prepares: the apply
            // path stamps each key's partition and must not fail on a key it cannot route either.
            Assert.True(await managers[0].KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(Partition, [], intents, ct));

            await WaitUntilAsync(() =>
            {
                foreach (KahunaManager manager in managers)
                    if (manager.KeyValues.DurablePreparedIntentStore.LiveIntentCount < UnroutableIntents)
                        return false;

                return true;
            }, timeoutMs: 30_000);

            foreach (KahunaManager manager in managers)
            {
                for (int partition = 1; partition <= Partitions; partition++)
                {
                    (KeyValueResponseType type, KeyValueApplyFingerprint fingerprint) = await RetryOnMustRetryAsync(
                        () => manager.GetPartitionApplyFingerprint(partition, ct), r => r.Item1);

                    Assert.Equal(KeyValueResponseType.Get, type);

                    // An intent no descriptor covers belongs to no partition this node compares.
                    Assert.Equal(0, fingerprint.LiveIntents);
                }
            }

            Assert.Equal(0, Interlocked.Read(ref routingFailures));
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= onFirstChance;

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>The throwing resolution keeps naming the key and its space: request paths report it.</summary>
    [Fact]
    public async Task Locate_OverAKeyNoDescriptorCovers_StillRefusesByKeyAndSpace()
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager manager = (KahunaManager)kahunas[0];

        try
        {
            manager.RegisterKeyRange("ur:t");

            KahunaServerException refused = Assert.Throws<KahunaServerException>(() => manager.LocateRange("ur:t/k0001"));
            Assert.Equal("No range descriptor covers key 'ur:t/k0001' in key-range space 'ur:t'.", refused.Message);

            // A hash-routed key is unaffected by the registration.
            Assert.InRange(manager.LocateRange("plain/k0001").PartitionId, 1, Partitions);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}

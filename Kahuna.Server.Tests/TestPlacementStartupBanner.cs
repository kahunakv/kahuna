using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Kahuna.Server.Communication.Internode;
using Kommander;
using Kommander.Communication.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies the placement startup banner: on its first committed partition-map application every
/// node logs — once, at Information — the mode it runs in (effective replication factor, rebalancer
/// state) and how many of the cluster's partitions it hosts locally, in both full-replication and
/// per-partition placement modes.
/// </summary>
public sealed partial class TestPlacementStartupBanner : BaseCluster
{
    private const string BannerPrefix = "Partition placement:";

    [GeneratedRegex(@"hosting (\d+) of (\d+) partitions")]
    private static partial Regex HostingRegex();

    private static Regex HostingPattern => HostingRegex();

    private static List<string> Banners(CapturingKahunaLogger capture) =>
        [.. capture.Lines.Where(l => l.StartsWith(BannerPrefix, StringComparison.Ordinal))];

    /// <summary>
    /// Records every Information+ line logged through the shared per-cluster kahuna logger so the
    /// test can assert on the banner without a live sink. Scopes and level filtering beyond
    /// Information are irrelevant here.
    /// </summary>
    private sealed class CapturingKahunaLogger : ILogger<IKahuna>
    {
        public readonly ConcurrentQueue<string> Lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                Lines.Enqueue(formatter(state, exception));
        }
    }

    [Fact]
    public async Task Banner_FullReplication_EveryNodeHostsEveryPartitionOnce()
    {
        CapturingKahunaLogger capture = new();

        (IRaft raft1, IRaft raft2, IRaft raft3, _, _, _) = await AssembleThreNodeCluster(
            "memory", 4, NullLogger<IRaft>.Instance, capture);

        // The banner fires on the map-application thread just after the initialized signal that
        // completes JoinCluster, so give the handlers a bounded window to run.
        await WaitUntilAsync(() => Banners(capture).Count >= 3);

        List<string> banners = Banners(capture);

        Assert.Equal(3, banners.Count); // one per node, never repeated on later map applications
        foreach (string line in banners)
        {
            Assert.Contains("replication factor 0 (full replication)", line, StringComparison.Ordinal);
            Assert.Contains("rebalancer disabled", line, StringComparison.Ordinal);

            Match m = HostingPattern.Match(line);
            Assert.True(m.Success, $"banner did not report a hosted count: {line}");
            Assert.Equal(m.Groups[2].Value, m.Groups[1].Value); // full replication: hosts all of them
            Assert.Equal("4", m.Groups[2].Value);
        }

        await LeaveCluster(raft1, raft2, raft3);
    }

    [Fact]
    public async Task Banner_ReplicationFactorOne_ReportsRestrictedHostedCounts()
    {
        const int partitions = 6;

        CapturingKahunaLogger capture = new();
        InMemoryCommunication raftComm = new();
        MemoryInterNodeCommmunication interComm = new();

        (IRaft raft1, IKahuna kahuna1) = BuildNode(interComm, raftComm, "memory", 1, 8001,
            ["localhost:8002", "localhost:8003"], NullLogger<IRaft>.Instance, capture, partitions, replicationFactor: 1);
        (IRaft raft2, IKahuna kahuna2) = BuildNode(interComm, raftComm, "memory", 2, 8002,
            ["localhost:8001", "localhost:8003"], NullLogger<IRaft>.Instance, capture, partitions, replicationFactor: 1);
        (IRaft raft3, IKahuna kahuna3) = BuildNode(interComm, raftComm, "memory", 3, 8003,
            ["localhost:8001", "localhost:8002"], NullLogger<IRaft>.Instance, capture, partitions, replicationFactor: 1);

        interComm.SetNodes(new()
        {
            { "localhost:8001", kahuna1 },
            { "localhost:8002", kahuna2 },
            { "localhost:8003", kahuna3 }
        });

        raftComm.SetNodes(new()
        {
            { "localhost:8001", raft1 },
            { "localhost:8002", raft2 },
            { "localhost:8003", raft3 }
        });

        CancellationToken ct = TestContext.Current.CancellationToken;
        await Task.WhenAll(raft1.JoinCluster(ct), raft2.JoinCluster(ct), raft3.JoinCluster(ct));

        // The banner fires on the map-application thread just after the initialized signal that
        // completes JoinCluster, so give the handlers a bounded window to run.
        await WaitUntilAsync(() => Banners(capture).Count >= 3);

        List<string> banners = Banners(capture);

        Assert.Equal(3, banners.Count);

        int hostedTotal = 0;
        foreach (string line in banners)
        {
            Assert.Contains("replication factor 1 (per-partition placement)", line, StringComparison.Ordinal);
            Assert.Contains("rebalancer disabled", line, StringComparison.Ordinal);

            Match m = HostingPattern.Match(line);
            Assert.True(m.Success, $"banner did not report a hosted count: {line}");
            Assert.Equal(partitions.ToString(), m.Groups[2].Value);
            hostedTotal += int.Parse(m.Groups[1].Value);
        }

        // Replication factor 1: every data partition is hosted by exactly one of the three nodes,
        // so the per-node hosted counts partition the full set.
        Assert.Equal(partitions, hostedTotal);

        await LeaveCluster(raft1, raft2, raft3);
    }
}

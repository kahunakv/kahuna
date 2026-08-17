using System.Collections.Concurrent;
using System.Text;
using Kahuna.Communication.External;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A graceful decommission of a placed node has to evacuate that node's replicas onto survivors
/// *before* its removal commits. Without the drain the roster shrinks while the committed map still
/// names the departed endpoint, and the operator who trusted the response and stopped the process
/// has silently dropped a replica from every range the node held.
/// </summary>
public sealed class TestPlacementDrain : BaseCluster
{
    private const int Nodes = 6;
    private const int Partitions = 4;
    private const int PlacedRf = 3;

    /// <summary>Records the Information lines every node emits, so hosted-set transitions are visible.</summary>
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

    private static int Count(CapturingKahunaLogger capture, string prefix) =>
        capture.Lines.Count(l => l.StartsWith(prefix, StringComparison.Ordinal));

    /// <summary>
    /// The endpoints the committed map still names as replicas of any data partition, as seen by
    /// <paramref name="observer"/>.
    /// </summary>
    private static HashSet<string> PlacedEndpoints(IRaft observer)
    {
        HashSet<string> endpoints = new(StringComparer.Ordinal);

        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
            foreach (RaftReplica replica in observer.GetPartitionReplicas(partitionId))
                endpoints.Add(replica.Endpoint);

        return endpoints;
    }

    [Fact]
    public async Task GracefulLeave_EvacuatesReplicasBeforeTheRemovalCommits()
    {
        CapturingKahunaLogger capture = new();

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions,
            NullLogger<IRaft>.Instance, capture,
            PlacedRf, enablePlacementRebalancer: true);

        CancellationToken ct = TestContext.Current.CancellationToken;

        // Seed one key per partition so the departing node actually holds state worth evacuating,
        // and so the survivors can be checked for it afterwards.
        Dictionary<string, byte[]> seeded = [];
        for (int i = 0; i < Partitions * 3; i++)
        {
            string key = $"drain/key{i}";
            byte[] value = Encoding.UTF8.GetBytes($"value{i}");

            (KeyValueResponseType type, _, _) = await RetryTransient(
                () => kahunas[0].LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, type);
            seeded[key] = value;
        }

        // Leave from a node that actually hosts something; a node holding no replica would drain
        // vacuously and prove nothing.
        int departingIndex = Array.FindIndex(rafts, r =>
            Enumerable.Range(1, Partitions).Any(r.HostsPartition));
        Assert.True(departingIndex >= 0, "no node hosts a data partition");

        IRaft departing = rafts[departingIndex];
        string departingEndpoint = departing.GetLocalEndpoint();
        IRaft survivor = rafts[(departingIndex + 1) % Nodes];

        Assert.Contains(departingEndpoint, PlacedEndpoints(survivor));

        int gainedBefore = Count(capture, "Started hosting");

        LeaveClusterResult result = await ClusterLeave.ExecuteAsync(departing, ct);

        Assert.Equal(LeaveClusterOutcome.Committed, result.Outcome);
        Assert.True(result.Left);
        Assert.True(result.Drained, "a node holding replicas must report a completed drain");

        // The committed map must already have stopped naming it — this is the ordering the drain
        // exists to guarantee, so it is asserted immediately rather than after a wait.
        Assert.DoesNotContain(departingEndpoint, PlacedEndpoints(survivor));

        // ...and every range still has its replicas, on survivors.
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            IReadOnlyList<RaftReplica> replicas = survivor.GetPartitionReplicas(partitionId);
            Assert.Equal(PlacedRf, replicas.Count(r => r.Role == RaftReplicaRole.Voter));
        }

        // Node-side evidence that replicas moved rather than the map being rewritten underneath.
        Assert.True(
            Count(capture, "Started hosting") > gainedBefore,
            "a gaining node should have logged that it started hosting an evacuated range");

        // The evacuated data is still readable from a surviving node.
        IKahuna reader = kahunas[(departingIndex + 1) % Nodes];
        foreach ((string key, byte[] expected) in seeded)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryTransient(
                () => reader.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(expected, entry!.Value);
        }

        await Task.WhenAll(rafts.Select(LeaveCluster));
    }

    /// <summary>
    /// Under full replication no range names any node in particular, so there is nothing to
    /// evacuate: the leave still commits, and reports that no drain happened rather than claiming
    /// one. An operator reading <c>Drained</c> as "durability was preserved" must not be told yes
    /// when nothing moved.
    /// </summary>
    [Fact]
    public async Task GracefulLeave_UnderFullReplication_CommitsWithoutClaimingADrain()
    {
        (IRaft[] rafts, _) = await AssembleCluster(
            Nodes, "memory", Partitions,
            NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance);

        CancellationToken ct = TestContext.Current.CancellationToken;

        LeaveClusterResult result = await ClusterLeave.ExecuteAsync(rafts[0], ct);

        Assert.Equal(LeaveClusterOutcome.Committed, result.Outcome);
        Assert.True(result.Left);
        Assert.False(result.Drained);

        await Task.WhenAll(rafts.Skip(1).Select(LeaveCluster));
    }

    /// <summary>
    /// Retries an operation while it answers a transient routing/replication outcome: a placed
    /// cluster forwards to leaders that may still be warming up, and those answers are
    /// guaranteed effect-free.
    /// </summary>
    private static async Task<T> RetryTransient<T>(Func<Task<T>> operation, Func<T, KeyValueResponseType> classify)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        T result = await operation();

        for (int attempt = 0; attempt < 100; attempt++)
        {
            KeyValueResponseType outcome = classify(result);
            if (outcome is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication))
                return result;

            await Task.Delay(50, ct);
            result = await operation();
        }

        return result;
    }
}

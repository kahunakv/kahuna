using System.Diagnostics.Metrics;
using System.Text;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A standalone embedded node's only peers are the two phantom witnesses, and the leader's
/// steady-state message to them is an empty heartbeat. What a witness reports about its own commit
/// frontier in that ACK is therefore what the leader believes about it for as long as the node is
/// idle — which is most of its life.
///
/// <para>Reporting a payload-derived frontier made every empty heartbeat report 0, and a follower's
/// self-report is recorded last-writer-wins, so the leader read a whole-log gap on every heartbeat
/// on every partition: a WAL range read and a re-shipped batch of history the witness discards, and
/// — once compaction has moved the readable floor above the anchor, which is where any long-lived
/// node ends up — a batch refused as non-contiguous plus a real partition-state export streamed into
/// a transport that rejects it. All of it on a node with no real followers at all.</para>
/// </summary>
public sealed class TestEmbeddedWitnessCommitFrontier
{
    private const string SampleKey = "tenant/table/frontier-key-0";

    private readonly ILoggerFactory loggerFactory;

    public TestEmbeddedWitnessCommitFrontier(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task TestIdleNodeNeverRecordsAWitnessBehindTheLeaderCommitIndex()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);
        await node.WaitForLeaderForKeyAsync(SampleKey, TestContext.Current.CancellationToken);

        int partitionId = node.Raft.GetPartitionKey(SampleKey);

        // Commit something in this term. Without it the leader's committed index equals the floor it
        // was elected at, and the idle-tail trigger — the one this regression is about — is gated off
        // regardless of what the witnesses report.
        for (int i = 0; i < 8; i++)
            await SetValue(node, $"tenant/table/frontier-key-{i}", $"value-{i}");

        long committedAfterWrites = node.Raft.GetCommitIndex(partitionId);
        Assert.True(committedAfterWrites > 0, $"expected committed entries on partition {partitionId}, got {committedAfterWrites}");

        // Then stop writing and sample the leader's belief about each witness throughout the window,
        // rather than once at the end. A single end-of-window reading is not evidence: with the
        // payload-derived report the recorded frontier oscillates — every empty heartbeat drops it to
        // 0, and the pointless catch-up batch that follows lifts it back — so where it happens to sit
        // when the window closes is a coin flip. The invariant is that it never dips at all.
        //
        // Heartbeats are counted over the same window so one that elapsed without any would fail
        // loudly instead of passing vacuously. (The counter is process-wide, so a concurrent embedded
        // node's heartbeats can inflate it; it is read only as a lower bound.)
        int heartbeats = 0;
        long lowestFrontier = long.MaxValue;
        string lowestWitness = "";

        using (MeterListener listener = new())
        {
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "Kommander" && instrument.Name == "raft.heartbeats_sent_total")
                    meterListener.EnableMeasurementEvents(instrument);
            };

            listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key == "partition_id" && tag.Value is int taggedPartition && taggedPartition == partitionId)
                        Interlocked.Add(ref heartbeats, (int)measurement);
                }
            });

            listener.Start();

            long deadline = Environment.TickCount64 + (long)IdleWindow(node).TotalMilliseconds;

            while (Environment.TickCount64 < deadline)
            {
                foreach (RaftNode witness in EmbeddedRaftCommunication.Witnesses)
                {
                    long? frontier = await node.Raft.GetFollowerLagAsync(partitionId, witness.Endpoint);

                    Assert.NotNull(frontier);

                    if (frontier < lowestFrontier)
                    {
                        lowestFrontier = frontier.Value;
                        lowestWitness = witness.Endpoint;
                    }
                }

                await Task.Delay(25, TestContext.Current.CancellationToken);
            }
        }

        Assert.True(heartbeats >= 3, $"expected several heartbeats on partition {partitionId} during the idle window, saw {heartbeats}");

        Assert.True(
            lowestFrontier >= committedAfterWrites,
            $"{lowestWitness} was recorded at commit frontier {lowestFrontier} while idle, below the leader's committed index {committedAfterWrites}");

        // Nothing may have been shipped to, or produced for, a peer that discards everything: no
        // refused anchored batch, and no partition-state export. Checked on the system partition too,
        // which heartbeats on its own cadence.
        AssertNoWitnessCatchUpTraffic(node, RaftSystemConfig.SystemPartition);
        AssertNoWitnessCatchUpTraffic(node, partitionId);
    }

    [Fact]
    public async Task TestStandaloneNodeTurnsBackfillOffRatherThanRaisingItsThreshold()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        // A large threshold is not a disable switch — it gates only the actively-behind trigger, so
        // the idle-tail and crash-restart triggers still fire at a witness the moment writes pause.
        Assert.False(node.Raft.Configuration.BackfillEnabled);
        Assert.NotEqual(int.MaxValue, node.Raft.Configuration.BackfillThreshold);

        // Witness-only quorums are the standalone constructor's business; a node given real peers
        // through the cluster constructor must still catch its followers up.
        Assert.True(EmbeddedKahunaNode.CreateRaftConfiguration(new()).BackfillEnabled);
    }

    private static void AssertNoWitnessCatchUpTraffic(EmbeddedKahunaNode node, int partitionId)
    {
        IReadOnlyList<RaftBackfillStatus> backfills = node.Raft.GetBackfillStatuses(partitionId);
        IReadOnlyList<RaftSnapshotStatus> snapshots = node.Raft.GetSnapshotStatuses(partitionId);

        foreach (RaftNode witness in EmbeddedRaftCommunication.Witnesses)
        {
            Assert.DoesNotContain(backfills, status => status.FollowerEndpoint == witness.Endpoint);
            Assert.DoesNotContain(snapshots, status => status.FollowerEndpoint == witness.Endpoint);
        }
    }

    /// <summary>
    /// Long enough to cover a good number of heartbeat rounds, and stretched by the same environment
    /// knob the cluster harness uses so a loaded machine does not run out of window.
    /// </summary>
    private static TimeSpan IdleWindow(EmbeddedKahunaNode node)
    {
        double scale = double.TryParse(Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE"), out double parsed) && parsed >= 1.0
            ? parsed
            : 1.0;

        return TimeSpan.FromMilliseconds(node.Raft.Configuration.HeartbeatInterval.TotalMilliseconds * 20 * scale);
    }

    private static async Task SetValue(EmbeddedKahunaNode node, string key, string value)
    {
        (KeyValueResponseType response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            Encoding.UTF8.GetBytes(value),
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);
    }
}

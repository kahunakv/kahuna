using System.Security.Cryptography;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A follower-side snapshot install that lands while a log tail is still pending above its boundary, with
/// one-phase bundles in that tail. The install replaces the partition's replicated committed-head ledger
/// wholesale, and the apply-time gate judges every bundle against that ledger, so an install that seeded the
/// wrong slice would make one replica decide a bundle differently from the rest — the one thing the design may
/// never do.
///
/// <para>Parity across an install is proven at the store level by <c>TestCommittedHeadLedger</c>, which hands a
/// snapshot and a tail to a store directly. This drives the install through the node: the payload is the
/// leader's own <c>ExportPartitionState</c>, and it is delivered to the follower's
/// <c>ReceiveInstallSnapshot</c> — the same entry point a leader's transfer chunk lands on — so the receive
/// session, the executor install, the consumer import, the durable WAL boundary and the apply-cursor seeding
/// all run for real. Consumer delivery is held for the whole window, so the tail above the boundary is
/// genuinely undelivered when the install arrives rather than incidentally so.</para>
///
/// <para><b>The fixture plays the sender.</b> A leader only sends a snapshot to a follower that has fallen
/// below its WAL compaction floor, and that floor only advances behind Kahuna's checkpoint cadence — minutes of
/// traffic, not seconds. What is under test is the receiving node's behaviour, and it is driven exactly as the
/// sender drives it.</para>
///
/// <para>The comparison replica is the third node, which never installed anything. It applied the same log the
/// ordinary way, so its ledger slice, its watermark and its values are what the installed node must converge
/// to.</para>
///
/// <para><b>Not covered here.</b> The conflicting-term twin — the tail's term disagrees with the boundary, the
/// suffix is truncated and the discarded entries never reach the consumer — needs an election placed between
/// the tail and the boundary, which a cluster fixture cannot time; Kommander pins that decision directly
/// against its own WAL. The ledger-less snapshot's fail-closed load is pinned at the store.</para>
/// </summary>
public sealed class TestOnePhaseSnapshotInstallOverPendingTail : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    /// <summary>The one data partition every key in this fixture routes to, so all traffic feeds one slice.</summary>
    private const int Partition = 1;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ITestOutputHelper output;

    public TestOnePhaseSnapshotInstallOverPendingTail(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        return index;
    }

    /// <summary>Fresh keys that all route to <paramref name="partition"/>, so every write feeds one ledger slice.</summary>
    private static string[] KeysOnPartition(KahunaManager probe, int partition, string tag, int count)
    {
        List<string> keys = new(count);
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; keys.Count < count && i < 65_536; i++)
        {
            string candidate = $"{tag}{i}{random}/k";
            if (probe.KeyValues.LocateDurablePartition(candidate).PartitionId == partition)
                keys.Add(candidate);
        }

        Assert.Equal(count, keys.Count);
        return [.. keys];
    }

    /// <summary>
    /// One read-modify-write transaction on <paramref name="key"/>. The read makes the write carry a validated
    /// base, which is what the apply-time gate judges against the partition's committed-head ledger — so the
    /// entries this leaves in the tail are entries an install could make a replica judge differently.
    /// </summary>
    private static async Task ReadModifyWrite(KahunaManager session, string key, int round, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = key,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 30_000
            }, ct);

        if (startType != KeyValueResponseType.Set)
            return;

        await session.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, System.Text.Encoding.UTF8.GetBytes($"v{round}"), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        if (writeType != KeyValueResponseType.Set)
        {
            await session.LocateAndRollbackTransaction(handle, ct);
            return;
        }

        // A contended round can legitimately answer MustRetry or Aborted; this workload exists to move the log,
        // and the parity assertions do not depend on any single round's outcome.
        await session.LocateAndCommitTransaction(handle, ct);
    }

    /// <summary>
    /// The Raft term the partition's leader is currently proposing in, read from a committed proposal's own
    /// reply through Kommander's reply hold. A snapshot boundary is stamped with the term of the entry at its
    /// index, and a wrong term silently turns the retain decision into a truncation — so the fixture reads the
    /// term rather than assuming the cluster never held an election.
    /// </summary>
    private static async Task<long> ReadCurrentTerm(
        IRaft raft, KahunaManager session, string key, CancellationToken ct)
    {
        TaskCompletionSource<long> term = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using IDisposable registration = raft.HoldCommittedProposalRepliesForTesting(Partition, reply =>
        {
            term.TrySetResult(reply.Term);
            reply.Release();
        });

        while (!term.Task.IsCompleted)
        {
            await ReadModifyWrite(session, key, round: -1, ct);
            await Task.Delay(20, ct);
        }

        return await term.Task;
    }

    [Fact]
    public async Task SnapshotInstallOverAPendingTail_ConvergesWithAReplicaThatNeverInstalled()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = true);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        int installee = -1;
        IDisposable? gate = null;

        try
        {
            string[] keys = KeysOnPartition(managers[0], Partition, "snapinst", count: 4);

            int leader = await LeaderIndexOf(Partition, rafts, ct);
            installee = (leader + 1) % Nodes;
            int control = (leader + 2) % Nodes;

            KahunaManager session = managers[leader];

            // Warm-up: the ledger holds a head for every key, and the log holds the entries that produced them.
            for (int round = 0; round < 6; round++)
                foreach (string key in keys)
                    await ReadModifyWrite(session, key, round, ct);

            long term = await ReadCurrentTerm(rafts[leader], session, keys[0], ct);

            // Deliver nothing to this node's consumer from here on. Replication, acks and the commit frontier are
            // untouched, so its log keeps growing while its apply cursor stays where it is — which is what puts a
            // pending tail above the boundary installed below.
            Assert.Equal(RaftOperationStatus.Success, await rafts[installee].HoldConsumerAppliesForTesting(Partition, ct));

            long heldAt = rafts[installee].GetCommitIndex(Partition);

            // Quiesce, so the boundary index and the exported state are captured at the same log position.
            await WaitForStableCommitIndex(rafts[leader], ct);
            long snapshotIndex = rafts[leader].GetCommitIndex(Partition);
            byte[] payload = await ExportPartitionState(session, snapshotIndex, ct);

            // Now grow the tail: everything from here lands above the boundary the install is about to write.
            for (int round = 6; round < 30; round++)
                foreach (string key in keys)
                    await ReadModifyWrite(session, key, round, ct);

            await WaitUntilAsync(() => rafts[installee].GetCommitIndex(Partition) > snapshotIndex, timeoutMs: 30_000);

            SnapshotInstallSignal? observed = null;
            gate = rafts[installee].SetSnapshotInstallGateForTesting(Partition, SnapshotInstallPhase.AfterImport, signal =>
            {
                observed ??= signal;
                return ValueTask.CompletedTask;
            });

            output.WriteLine($"install: term {term}, boundary {snapshotIndex}, follower commit {rafts[installee].GetCommitIndex(Partition)}, payload {payload.Length} bytes");

            SnapshotResponse response = await ((RaftManager)rafts[installee]).ReceiveInstallSnapshot(new SnapshotRequest
            {
                SessionId = Guid.NewGuid().ToString("N"),
                PartitionId = Partition,
                SnapshotIndex = snapshotIndex,
                LeaderTerm = term,
                LastIncludedTerm = term,
                LeaderEndpoint = EndpointOf(leader),
                FollowerEndpoint = EndpointOf(installee),
                ChunkIndex = 0,
                IsLast = true,
                Data = payload,
                Kind = SnapshotKind.PartitionState,
                SnapshotChecksum = Convert.ToHexString(SHA256.HashData(payload))
            }, ct);

            Assert.True(response.Success, "the follower refused the snapshot install");

            Assert.NotNull(observed);
            SnapshotInstallSignal signal = observed!;
            Assert.Equal(Partition, signal.PartitionId);
            Assert.Equal(snapshotIndex, signal.SnapshotIndex);
            Assert.True(
                signal.LocalMaxLogId > signal.SnapshotIndex,
                $"the install must land under a pending tail: local max log {signal.LocalMaxLogId}, boundary {signal.SnapshotIndex}");
            Assert.True(
                signal.LastAppliedIndex <= heldAt,
                $"the consumer must not have advanced while applies were held: applied {signal.LastAppliedIndex}, held at {heldAt}");

            // Let the tail through; both replicas now converge on the same log.
            Assert.Equal(RaftOperationStatus.Success, await rafts[installee].ResumeConsumerAppliesForTesting(Partition, ct));

            KahunaManager installed = managers[installee];
            KahunaManager neverInstalled = managers[control];

            // The verdict parity: the replica that installed a snapshot and then applied the tail holds the same
            // committed-head slice, at the same log position, as the replica that only ever applied the log.
            await WaitUntilAsync(() =>
                installed.DurablePreparedIntentStore.GetLedgerWatermark(Partition)
                    == neverInstalled.DurablePreparedIntentStore.GetLedgerWatermark(Partition),
                timeoutMs: 60_000);

            Assert.Equal(
                neverInstalled.DurablePreparedIntentStore.SnapshotLedger(Partition),
                installed.DurablePreparedIntentStore.SnapshotLedger(Partition));

            // And the values the slice describes are the same on both.
            foreach (string key in keys)
            {
                (KeyValueResponseType controlType, ReadOnlyKeyValueEntry? controlEntry) = await RetryOnMustRetryAsync(
                    () => neverInstalled.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1);

                (KeyValueResponseType installedType, ReadOnlyKeyValueEntry? installedEntry) = await RetryOnMustRetryAsync(
                    () => installed.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1);

                Assert.Equal(controlType, installedType);
                Assert.Equal(controlEntry!.Revision, installedEntry!.Revision);
                Assert.Equal(controlEntry.Value, installedEntry.Value);
            }
        }
        finally
        {
            gate?.Dispose();

            if (installee >= 0)
            {
                try { await rafts[installee].ResumeConsumerAppliesForTesting(Partition, ct); }
                catch (Exception) { /* teardown: the partition may already be stopping */ }
            }

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>Waits until the partition's commit index stops moving, so the index and the export agree.</summary>
    private static async Task WaitForStableCommitIndex(IRaft raft, CancellationToken ct)
    {
        long previous = -1;
        await WaitUntilAsync(async () =>
        {
            long current = raft.GetCommitIndex(Partition);
            await Task.Delay(200, ct);
            bool stable = current == previous && current == raft.GetCommitIndex(Partition);
            previous = current;
            return stable;
        }, timeoutMs: 30_000);
    }

    private static async Task<byte[]> ExportPartitionState(KahunaManager leader, long upToIndex, CancellationToken ct)
    {
        await using Stream stream = await leader.KeyValues.PartitionStateTransfer.ExportPartitionState(Partition, upToIndex, ct);
        using MemoryStream buffer = new();
        await stream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }
}

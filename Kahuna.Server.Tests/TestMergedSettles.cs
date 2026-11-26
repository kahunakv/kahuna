using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The settles of concurrent durable commits that leave in one merged entry: every committed value is installed and
/// served on one node, after a restart of that node, and on every replica of a three-node group. Each test proves
/// from the log itself that at least one entry carried the settles of several transactions.
/// </summary>
public sealed class TestMergedSettles : BaseCluster, IDisposable
{
    private readonly ILoggerFactory loggerFactory;

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_mergedsettles_" + Guid.NewGuid().ToString("N"));

    public TestMergedSettles(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
        catch (IOException)
        {
            // A file still held open by a node that stops late; the temp directory is cleaned by the system.
        }
    }

    /// <summary>Holds each batch that carries a settle for a short time, so the settles of the commits that end
    /// meanwhile queue behind it and leave together in the next batch.</summary>
    private sealed class SettleHoldingExecutor(IPartitionBatchExecutor inner) : IPartitionBatchExecutor
    {
        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            foreach (RaftProposalEntry entry in entries)
            {
                if (entry.Type == ReplicationTypes.PreparedIntent && !PreparedIntentStore.DecodeDelta(entry.Data).Any(c => c is PrepareIntentCommand))
                {
                    await Task.Delay(15, cancellationToken);
                    break;
                }
            }

            return await inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    private static byte[] Script(int transaction) =>
        Encoding.UTF8.GetBytes($"BEGIN SET `merged/{transaction}/a` 'value-{transaction}' SET `merged/{transaction}/b` 'value-{transaction}' COMMIT END");

    private static IEnumerable<string> Keys(int transactions)
    {
        for (int i = 0; i < transactions; i++)
        {
            yield return $"merged/{i}/a";
            yield return $"merged/{i}/b";
        }
    }

    private static string ValueOf(string key) => "value-" + key.Split('/')[1];

    /// <summary>The settle entries of the log of every partition of <paramref name="raft"/> that carry the commands of
    /// more than one transaction, and every key that a materializing settle of the log installs.</summary>
    private static (int MergedEntries, HashSet<string> InstalledKeys) ScanSettles(IRaft raft)
    {
        int merged = 0;
        HashSet<string> installed = new(StringComparer.Ordinal);

        foreach (RaftPartitionRange partition in raft.GetPartitionMap())
        {
            foreach (RaftLog log in raft.WalAdapter.ReadLogsRange(partition.PartitionId, 1))
            {
                if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is not { Length: > 0 })
                    continue;

                PreparedIntentCommand[] commands = PreparedIntentStore.DecodeDelta(log.LogData);
                if (commands.Any(c => c is PrepareIntentCommand))
                    continue;

                HashSet<HLCTimestamp> transactions = [];
                foreach (PreparedIntentCommand command in commands)
                {
                    if (command is ResolveIntentCommand resolve)
                    {
                        transactions.Add(resolve.TransactionId);
                        if (resolve is { Commit: true, MaterializeOnResolve: true })
                            installed.Add(resolve.Key);
                    }
                }

                if (transactions.Count > 1)
                    merged++;
            }
        }

        return (merged, installed);
    }

    private static async Task CommitConcurrentlyAsync(IKahuna kahuna, int transactions)
    {
        KeyValueTransactionResult[] results = await Task.WhenAll(Enumerable.Range(0, transactions).Select(i =>
            RetryOnMustRetryAsync(() => kahuna.TryExecuteTransactionScript(Script(i), null, null), r => r.Type)));

        Assert.All(results, result => Assert.Equal(KeyValueResponseType.Set, result.Type));
    }

    private static async Task AssertReadsBackAsync(IKahuna kahuna, int transactions, CancellationToken ct)
    {
        foreach (string key in Keys(transactions))
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(ValueOf(key), Encoding.UTF8.GetString(entry!.Value!));
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 20_000;
        while (!condition())
        {
            if (Environment.TickCount64 > deadline)
                Assert.Fail("Timed out waiting until " + what);

            await Task.Delay(20, ct);
        }
    }

    [Fact]
    public async Task OneNode_ConcurrentCommits_SettleInMergedEntries_AndServeEveryValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int transactions = 48;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            DurableMaterializeOnResolve = true,
            DurableMergeAdjacentSettles = true,
            WriteBatchExecutorDecorator = inner => new SettleHoldingExecutor(inner)
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("merged/0/a", ct);

        await CommitConcurrentlyAsync(node.Kahuna, transactions);
        await AssertReadsBackAsync(node.Kahuna, transactions, ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, "every intent settled", ct);

        (int mergedEntries, HashSet<string> installed) = ScanSettles(node.Raft);
        Assert.True(mergedEntries > 0, "no settle entry carried more than one transaction");
        Assert.All(Keys(transactions), key => Assert.Contains(key, installed));

        // Served from the installed rows once the intents are gone.
        await AssertReadsBackAsync(node.Kahuna, transactions, ct);
    }

    [Fact]
    public async Task OneNode_MergedSettles_SurviveARestart()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int transactions = 32;

        string storagePath = Path.Combine(tempRoot, "store");
        string walPath = Path.Combine(tempRoot, "wal");
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        EmbeddedKahunaOptions Options(bool hold) => new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            InitialPartitions = 1,
            Storage = "sqlite",
            StoragePath = storagePath,
            StorageRevision = "merged",
            WalStorage = "sqlite",
            WalPath = walPath,
            WalRevision = "merged-wal",
            DurableMaterializeOnResolve = true,
            DurableMergeAdjacentSettles = true,
            WriteBatchExecutorDecorator = hold ? inner => new SettleHoldingExecutor(inner) : null
        };

        await using (EmbeddedKahunaNode node = new(Options(hold: true), loggerFactory))
        {
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("merged/0/a", ct);

            await CommitConcurrentlyAsync(node.Kahuna, transactions);

            KahunaManager manager = (KahunaManager)node.Kahuna;
            await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, "every intent settled", ct);

            Assert.True(ScanSettles(node.Raft).MergedEntries > 0, "no settle entry carried more than one transaction");
        }

        await using (EmbeddedKahunaNode restarted = new(Options(hold: false), loggerFactory))
        {
            await restarted.StartAsync(ct);
            await restarted.WaitForLeaderForKeyAsync("merged/0/a", ct);

            await AssertReadsBackAsync(restarted.Kahuna, transactions, ct);
        }
    }

    [Fact]
    public async Task Cluster_EveryReplicaInstallsTheValuesOfAMergedSettle()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int transactions = 40;

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 1, raftLogger, kahunaLogger, c =>
            {
                c.DurableMaterializeOnResolve = true;
                c.DurableMergeAdjacentSettles = true;

                // Settles that end during a round wait for the next dispatch together.
                c.KeyValueWritePostCompletionHoldMs = 5;
            });

        try
        {
            await CommitConcurrentlyAsync(kahuna1, transactions);
            await AssertReadsBackAsync(kahuna1, transactions, ct);

            IRaft[] rafts = [raft1, raft2, raft3];
            KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

            for (int r = 0; r < managers.Length; r++)
            {
                KahunaManager manager = managers[r];
                await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, $"replica {r} settles every intent", ct);

                foreach (string key in Keys(transactions))
                {
                    byte[] expected = Encoding.UTF8.GetBytes(ValueOf(key));
                    await WaitUntilAsync(
                        () => manager.PersistenceBackend.GetKeyValue(key) is { } entry && entry.Value is not null && entry.Value.AsSpan().SequenceEqual(expected),
                        $"replica {r} holds {key}", ct);
                }

                (int mergedEntries, HashSet<string> installed) = ScanSettles(rafts[r]);
                Assert.True(mergedEntries > 0, $"replica {r}: no settle entry carried more than one transaction");
                Assert.All(Keys(transactions), key => Assert.Contains(key, installed));
            }
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }
}

using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The one-phase bundled commit under a crash on a persistent WAL: a transaction commits through the single
/// [record init + prepare + decision] batch with apply-time validation on, the node dies after that batch is
/// durable but before the committed values materialize, and a cold restart replays the bundle from the WAL. The
/// acknowledged commit must survive: the record is Commit again after replay, the intents are pending again,
/// the recovery sweep finishes settlement, and reads serve the committed values from materialized state.
/// </summary>
public sealed class TestOnePhaseCrashRecovery
{
    private readonly ILoggerFactory loggerFactory;

    public TestOnePhaseCrashRecovery(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>Fails every batch carrying a post-decision materialization (an ordinary key/value record), so the
    /// bundle commits durably but the values never reach the WAL before the crash.</summary>
    private sealed class MaterializationBlockingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private int blocked;
        private int onePhaseBundles;

        public MaterializationBlockingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public int BlockedMaterializations => Volatile.Read(ref blocked);

        /// <summary>Batches that carried a [record, intent, record] run — the one-phase bundle's shape.</summary>
        public int OnePhaseBundles => Volatile.Read(ref onePhaseBundles);

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            for (int i = 0; i + 2 < entries.Count; i++)
            {
                if (entries[i].Type == ReplicationTypes.TransactionRecord
                    && entries[i + 1].Type == ReplicationTypes.PreparedIntent
                    && entries[i + 2].Type == ReplicationTypes.TransactionRecord)
                {
                    Interlocked.Increment(ref onePhaseBundles);
                    break;
                }
            }

            bool materialization = true;
            foreach (RaftProposalEntry entry in entries)
                materialization &= entry.Type == ReplicationTypes.KeyValues;

            if (materialization)
            {
                Interlocked.Increment(ref blocked);

                List<RaftEntryResult> failed = new(entries.Count);
                for (int i = 0; i < entries.Count; i++)
                    failed.Add(new RaftEntryResult(RaftOperationStatus.Errored, -1, HLCTimestamp.Zero));

                return Task.FromResult(new RaftBatchReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, failed));
            }

            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    private static EmbeddedKahunaOptions PersistentOptions(
        string storagePath, string walPath, Func<IPartitionBatchExecutor, IPartitionBatchExecutor>? decorator) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "one-phase-crash",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "one-phase-crash-wal",
        WalSyncWrites = true,
        DurableDeferredSettlement = true,
        OnePhaseApplyTimeValidation = true,
        CollectionInterval = TimeSpan.FromMinutes(10),
        WriteBatchExecutorDecorator = decorator
    };

    [Fact]
    public async Task OnePhaseCommit_SurvivesCrashBeforeMaterialization_RecoverySettles()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-onephase-crash-store-");
        string walPath = CreateTempDir("kahuna-onephase-crash-wal-");

        try
        {
            HLCTimestamp committedTx;

            // ── Step 1: commit through the one-phase bundle, then crash before the values materialize. ──
            {
                MaterializationBlockingExecutor? blocker = null;
                await using EmbeddedKahunaNode node = new(
                    PersistentOptions(storagePath, walPath, inner => blocker = new MaterializationBlockingExecutor(inner)), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync("op/k1", ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes("BEGIN SET `op/k1` 'alpha' SET `op/k2` 'beta' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, result.Type);

                // The bundle shape reached the executor: this commit took the single-batch path, not two-phase.
                Assert.True(blocker!.OnePhaseBundles >= 1, "the commit did not take the one-phase bundle");

                await WaitUntil(() => blocker.BlockedMaterializations > 0, ct);

                Assert.True(kahuna.DurableTransactionRecordStore.Count > 0, "the bundled decision must be durable before the crash");
                Assert.True(kahuna.DurablePreparedIntentStore.Count > 0, "the committed intents must still be pending before the crash");

                PreparedIntent pending = kahuna.DurablePreparedIntentStore.Get("op/k1")!;
                committedTx = pending.TransactionId;
                Assert.Equal(TransactionDecision.Commit, kahuna.DurableTransactionRecordStore.Get(committedTx, pending.Epoch)!.Decision);
            }

            // ── Step 2: cold restart over the same WAL. The bundle replays; recovery finishes settlement. ──
            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, decorator: null), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync("op/k1", ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                // The replayed bundle judged the same way it did live: the record is Commit, never Undecided or a
                // fresh rejection, and the intents are pending again.
                Assert.True(kahuna.DurablePreparedIntentStore.Count > 0, "pending intents must replay from the WAL");
                PreparedIntent replayed = kahuna.DurablePreparedIntentStore.Get("op/k1")!;
                Assert.Equal(committedTx, replayed.TransactionId);
                Assert.Equal(TransactionDecision.Commit, kahuna.DurableTransactionRecordStore.Get(committedTx, replayed.Epoch)!.Decision);

                await WaitUntil(async () =>
                {
                    await kahuna.KeyValues.RecoverPreparedIntents(ct);
                    return kahuna.DurablePreparedIntentStore.Count == 0;
                }, ct, timeoutMs: 30_000);

                (KeyValueResponseType t1, ReadOnlyKeyValueEntry? e1) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, "op/k1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, t1);
                Assert.Equal("alpha"u8.ToArray(), e1!.Value);

                (KeyValueResponseType t2, ReadOnlyKeyValueEntry? e2) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, "op/k2", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, t2);
                Assert.Equal("beta"u8.ToArray(), e2!.Value);
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static async Task WaitUntil(Func<bool> predicate, CancellationToken ct, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(50, ct);
        }
        Assert.True(predicate(), "condition not met in time");
    }

    private static async Task WaitUntil(Func<Task<bool>> predicate, CancellationToken ct, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (await predicate()) return;
            await Task.Delay(50, ct);
        }
        Assert.True(await predicate(), "condition not met in time");
    }
}

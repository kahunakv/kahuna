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
/// Deferred settlement, crash-recovery end to end on a persistent Raft WAL: a durable commit acknowledges as soon
/// as its decision record is durable, while materialization runs off the critical path. If the node dies after the
/// decision is durable but before the committed value materializes, the acknowledged commit must not be lost — a
/// cold restart replays the durable decision and its still-pending prepared intent from the WAL, and the recovery
/// sweep finishes settlement (materializes the committed value, then settles the intent).
///
/// This exercises the WAL-replay → recovery path a single-node process restart takes, which the in-memory
/// deferred/recovery tests never reach. The decision→materialization window is pinned deterministically (not by
/// racing a background task): the write-batch executor is decorated to fail the post-decision materialization
/// replicate (an ordinary key/value record) while letting the prepare and decision records through, so the durable
/// state left on disk is exactly a committed-but-unmaterialized intent.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDeferredSettlementCrashRecovery
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeferredSettlementCrashRecovery(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// Fails every batch carrying a post-decision materialization (an ordinary key/value record) so the committed
    /// value never reaches the WAL, leaving the prepared intent pending. Prepare/decision records — and everything
    /// else — pass straight through, so the canonical decision is still made durable. Counts the blocks so the test
    /// can wait until materialization has actually been attempted (it runs on a background task after the commit
    /// returns) before crashing.
    /// </summary>
    private sealed class MaterializationBlockingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private int blocked;

        public MaterializationBlockingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public int BlockedMaterializations => Volatile.Read(ref blocked);

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            // In this test the only ordinary key/value records are post-decision materializations of the durable
            // transaction's committed intents (there are no direct writes), so failing them fails materialization
            // without touching the prepare/decision records.
            if (entries.Any(e => e.Type == ReplicationTypes.KeyValues))
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
        string storagePath, string walPath, Func<IPartitionBatchExecutor, IPartitionBatchExecutor>? decorator, TimeSpan collectionInterval) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        // A stable, non-empty revision so the reconstructed node reopens the SAME database and WAL files (an empty
        // revision defaults to a fresh GUID per construction, which would open empty stores and never replay).
        StorageRevision = "crash-recovery",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "crash-recovery-wal",
        WalSyncWrites = true,
        DurableDeferredSettlement = true,
        CollectionInterval = collectionInterval,
        WriteBatchExecutorDecorator = decorator
    };

    [Fact]
    public async Task DurableCommit_SurvivesCrashBeforeMaterialization_RecoveryMaterializes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-crash-store-");
        string walPath = CreateTempDir("kahuna-crash-wal-");

        try
        {
            // ── Phase 1: commit a durable transaction, then crash after the decision is durable but before the
            // committed value materializes (materialization is failed by the executor decorator). ──
            {
                MaterializationBlockingExecutor? blocker = null;
                // A large collection interval keeps the recovery sweep from firing during phase 1; the point of this
                // phase is to leave the committed-but-unmaterialized intent on disk, not to recover it.
                EmbeddedKahunaOptions options = PersistentOptions(
                    storagePath, walPath, inner => blocker = new MaterializationBlockingExecutor(inner), TimeSpan.FromMinutes(10));

                await using EmbeddedKahunaNode node = new(options, loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync("crash/k1", ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes("BEGIN SET `crash/k1` 'alpha' SET `crash/k2` 'beta' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, result.Type);

                // The commit acknowledged: its decision record is durable. Materialization runs on a background task
                // after the commit returns, so wait until it has actually been attempted (and failed by the decorator).
                await WaitUntil(() => blocker!.BlockedMaterializations > 0, ct);

                // The committed-but-unmaterialized durable state is exactly what a crash-after-decision leaves: the
                // canonical decision record is durable, and the prepared intents linger unresolved (materialization
                // was blocked, so nothing settled).
                Assert.True(kahuna.DurableTransactionRecordStore.Count > 0, "decision record must be durable before the crash");
                Assert.True(kahuna.DurablePreparedIntentStore.Count > 0, "committed intents must still be pending (unmaterialized) before the crash");
            }

            // ── Phase 2: reconstruct the node on the SAME persistent WAL. Recovery must replay the durable decision
            // and its pending intent and finish settlement — no acknowledged commit lost. ──
            {
                // No decorator: materialization is allowed to succeed this time (the recovery sweep performs it).
                EmbeddedKahunaOptions options = PersistentOptions(storagePath, walPath, decorator: null, TimeSpan.FromMinutes(10));

                await using EmbeddedKahunaNode node = new(options, loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync("crash/k1", ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                // The durable committed-but-unmaterialized state survived the restart: the decision replayed from the
                // WAL and the intents are pending again (recovery has not run — the collection interval is long, and
                // an intent is only due for recovery once its recovery deadline passes).
                Assert.True(kahuna.DurableTransactionRecordStore.Count > 0, "decision record must survive the restart");
                Assert.True(kahuna.DurablePreparedIntentStore.Count > 0, "pending intents must replay from the WAL");

                // Drive the recovery sweep until it finishes settlement. An intent becomes eligible only once its
                // recovery deadline (frozen at commit) passes, so the sweep is a no-op until then; loop until the
                // committed values materialize and the intents settle (drain to zero).
                await WaitUntil(async () =>
                {
                    await kahuna.KeyValues.RecoverPreparedIntents(ct);
                    return kahuna.DurablePreparedIntentStore.Count == 0;
                }, ct, timeoutMs: 30_000);

                // With every intent settled, a read can only be served from materialized KV state — proving recovery
                // finished settlement and the acknowledged commit is visible on the recovering leader, not merely
                // durable in the log.
                (KeyValueResponseType t1, ReadOnlyKeyValueEntry? e1) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, "crash/k1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, t1);
                Assert.Equal("alpha"u8.ToArray(), e1!.Value);

                (KeyValueResponseType t2, ReadOnlyKeyValueEntry? e2) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, "crash/k2", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
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

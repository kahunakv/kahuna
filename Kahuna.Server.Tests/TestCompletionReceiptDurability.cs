
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Durability of persistent-participant completion receipts across a cold restart whose WAL no longer holds
/// the receipt-bearing committed log entry — the case WAL-tail replay cannot cover, because past the retention
/// floor that entry is compacted away. The background writer snapshots the receipt set to disk at checkpoint
/// time (before the floor advances), and the store reloads it on construction, so a re-commit after the restart
/// still answers <c>Committed</c> instead of the ambiguous <c>MustRetry</c>.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestCompletionReceiptDurability
{
    private const string StorageRevision = "receipt-dur";

    private static EmbeddedKahunaOptions Options(string storagePath, string walPath, string walRevision) => new()
    {
        Storage         = "sqlite",
        StoragePath     = storagePath,
        StorageRevision = StorageRevision,
        WalStorage      = "sqlite",
        WalPath         = walPath,
        WalRevision     = walRevision,
        WalSyncWrites   = false,
        InitialPartitions = 1,
        // Flush promptly and checkpoint soon so the receipt snapshot (written when the partition's WAL floor
        // advances) is reached within the test window.
        DirtyObjectsWriterDelay = 250,
        CheckpointInterval = TimeSpan.FromSeconds(1),
    };

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    [Fact]
    public async Task Receipt_SurvivesColdRestart_ViaCheckpointSnapshot_WhenWalCannotReplay()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-receiptdur-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-receiptdur-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);
        // The receipt snapshot is written per data partition at checkpoint time ("_p{partitionId}").
        bool ReceiptSnapshotWritten() =>
            Directory.GetFiles(storagePath, $"completionreceipts_{StorageRevision}_p*.snapshot").Length > 0;

        try
        {
            const string key = "receiptdur:key";
            byte[] value = "v"u8.ToArray();
            HLCTimestamp txId;
            HLCTimestamp ticket;

            await using (EmbeddedKahunaNode first = new(Options(storagePath, walPath, "receipt-dur-w1")))
            {
                await first.StartAsync(ct);

                int nodeId = first.Raft.GetLocalNodeId();
                txId = first.Raft.HybridLogicalClock.TrySendOrLocalEvent(nodeId);
                HLCTimestamp commitId = first.Raft.HybridLogicalClock.TrySendOrLocalEvent(nodeId);

                (KeyValueResponseType setType, _, _) = await first.Kahuna.TrySetKeyValue(
                    txId, key, value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Set, setType);
                (KeyValueResponseType prepType, HLCTimestamp t, _, _) = await first.Kahuna.TryPrepareMutations(
                    txId, commitId, key, KeyValueDurability.Persistent, recordAnchorKey: null);
                Assert.Equal(KeyValueResponseType.Prepared, prepType);
                ticket = t;
                (KeyValueResponseType commitType, _) = await first.Kahuna.TryCommitMutations(
                    txId, key, ticket, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Committed, commitType);

                // Flush the committed value to the backend, then wait for the checkpoint to snapshot the
                // receipt set to disk — that snapshot is what carries the receipt when the WAL cannot replay.
                await first.FlushAsync();
                long deadline = Environment.TickCount64 + 15_000;
                while (!ReceiptSnapshotWritten() && Environment.TickCount64 < deadline)
                    await Task.Delay(50, ct);
                Assert.True(ReceiptSnapshotWritten(), "completion-receipt snapshot was not written at checkpoint");
            }

            // Restart over the same storage (backend + receipt snapshot) but a FRESH WAL: there is nothing to
            // replay, so the only way the receipt can come back is the on-disk snapshot.
            await using EmbeddedKahunaNode second = new(Options(storagePath, walPath, "receipt-dur-w2"));
            await second.StartAsync(ct);

            // The committed value is served from the backend checkpoint.
            long readDeadline = Environment.TickCount64 + 15_000;
            (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await second.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            while (readType != KeyValueResponseType.Get && Environment.TickCount64 < readDeadline)
            {
                await Task.Delay(50, ct);
                (readType, entry) = await second.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            }
            Assert.Equal(KeyValueResponseType.Get, readType);
            Assert.Equal(value, entry!.Value);

            // The re-commit finds the reloaded receipt (the write intent is long gone with the old WAL) and
            // answers Committed — not the ambiguous MustRetry a lost receipt would force.
            (KeyValueResponseType recommit, _) = await second.Kahuna.TryCommitMutations(
                txId, key, ticket, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, recommit);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }
}

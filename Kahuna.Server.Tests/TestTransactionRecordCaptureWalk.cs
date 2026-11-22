using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The checkpoint capture of the transaction records walks the map of the store while records apply. The walk
/// is not a point-in-time cut. What a restart needs from the file is that every record applied before the
/// capture began is in it, in a state at or after that apply: the entries applied during the capture are above
/// the position the capture certifies, and a restart replays them.
/// </summary>
public sealed class TestTransactionRecordCaptureWalk : IDisposable
{
    private const int Partition = 2;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-record-walk-" + Guid.NewGuid().ToString("N"));

    public TestTransactionRecordCaptureWalk() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static void Initialize(TransactionRecordStore store, int i)
    {
        HLCTimestamp txId = Ts(1_000_000 + i);
        string anchor = $"walk/k{i:D7}";
        List<TransactionParticipantRef> manifest = [new(anchor, KeyValueDurability.Persistent), new($"walk/other{i:D7}", KeyValueDurability.Persistent)];

        store.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
    }

    private static void Commit(TransactionRecordStore store, int i)
    {
        HLCTimestamp txId = Ts(1_000_000 + i);
        store.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100)));
    }

    [Fact]
    public void ACaptureThatRunsWhileRecordsApply_KeepsEveryRecordFromBeforeIt_InAStateAtOrAfterItsApply()
    {
        const int before = 30_000;

        TransactionRecordStore store = new(dir, "rev", null);
        store.AttachAnchorResolver(_ => (Partition, 0));

        for (int i = 0; i < before; i++)
        {
            Initialize(store, i);
            Commit(store, i);
        }

        // The writer initializes and commits new transactions for the whole test, so the map grows and resizes
        // under the walk.
        using CancellationTokenSource stop = new();
        int committedByWriter = 0;
        Exception? writerFault = null;

        Thread writer = new(() =>
        {
            try
            {
                for (int i = 0; !stop.IsCancellationRequested && i < 60_000; i++)
                {
                    Initialize(store, before + i);
                    Commit(store, before + i);
                    Volatile.Write(ref committedByWriter, i + 1);
                }
            }
            catch (Exception ex)
            {
                writerFault = ex;
            }
        })
        { IsBackground = true, Name = "record-capture-writer" };

        writer.Start();

        int captures = 0;
        while (captures < 6 || (Volatile.Read(ref committedByWriter) < 5_000 && writer.IsAlive))
        {
            Assert.True(store.PersistSnapshot(Partition));
            captures++;
        }

        int committedBeforeLastCapture = Volatile.Read(ref committedByWriter);
        Assert.True(store.PersistSnapshot(Partition));

        stop.Cancel();
        writer.Join();
        Assert.Null(writerFault);

        TransactionRecordStore restarted = new(dir, "rev", null);

        // Committed before the last capture began: the file holds the commit, not an older state of the record.
        for (int i = 0; i < before + committedBeforeLastCapture; i++)
        {
            TransactionRecord? record = restarted.Get(Ts(1_000_000 + i), 1);
            Assert.True(record is not null, $"record {i} is missing from the file");
            Assert.Equal(TransactionDecision.Commit, record.Decision);
            Assert.Equal(2, record.Participants.Count);
        }

        // A record the walk met in the middle of its two applies is in the file as undecided or as committed.
        // The replay of its commit gives the same end.
        int total = before + Volatile.Read(ref committedByWriter);
        for (int i = before + committedBeforeLastCapture; i < total; i++)
        {
            if (restarted.Get(Ts(1_000_000 + i), 1) is null)
                Initialize(restarted, i);

            Commit(restarted, i);
            Assert.Equal(TransactionDecision.Commit, restarted.Get(Ts(1_000_000 + i), 1)!.Decision);
        }

        Assert.Equal(total, restarted.Count);

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{captures + 1} captures while {Volatile.Read(ref committedByWriter)} transactions committed; {restarted.Count} records after the replay");
    }
}

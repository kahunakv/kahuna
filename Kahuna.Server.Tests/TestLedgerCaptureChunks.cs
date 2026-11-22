using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// A capture of a ledger slice reads the settled identities one chunk at a time and releases the apply gate
/// between the chunks, so the applies of the partition run during the capture. These tests check what that
/// must not change: every identity the captured watermark retains is in the capture, whatever the applies do
/// meanwhile.
/// </summary>
public sealed class TestLedgerCaptureChunks : IDisposable
{
    private const int Partition = 3;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-ledger-chunks-" + Guid.NewGuid().ToString("N"));

    public TestLedgerCaptureChunks() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l, uint c = 0) => new(0, l, c);

    private static PreparedIntent MakeIntent(string key, HLCTimestamp transactionId, HLCTimestamp commit) => new(
        TransactionId: transactionId, Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: commit,
        State: KeyValueState.Set, Value: [1], Bucket: null,
        Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: true,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>Prepares, commits and removes one intent: the settlement that records a head and an identity.</summary>
    private static void Settle(PreparedIntentStore store, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent), Partition).Outcome);
        store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true), Partition);
        store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key), Partition);
    }

    /// <summary>A stream that runs an action on its first write: the moment a capture has written its first
    /// settled identities and has more of the memory to read.</summary>
    private sealed class FirstWriteHook(Action onFirstWrite) : MemoryStream
    {
        private Action? pending = onFirstWrite;

        public override void Write(byte[] buffer, int offset, int count)
        {
            base.Write(buffer, offset, count);
            Interlocked.Exchange(ref pending, null)?.Invoke();
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            base.Write(buffer);
            Interlocked.Exchange(ref pending, null)?.Invoke();
        }
    }

    [Fact]
    public void TheSweepWaits_WhileACaptureReadsTheMemory()
    {
        const int oldIdentities = 6_000;
        const int settledDuringCapture = 300;

        PreparedIntentStore source = new();

        // Old identities: one transaction of many keys, committed at 1,001.
        for (int i = 0; i < oldIdentities; i++)
            Settle(source, MakeIntent($"old/k{i:D6}", Ts(1_000), Ts(1_001)));

        // The watermark the capture reads: it still retains the old identities (the window is 60 s).
        Settle(source, MakeIntent("mark/k", Ts(50_000), Ts(50_001)));
        Assert.Equal(Ts(50_001), source.GetLedgerWatermark(Partition));
        Assert.Equal(oldIdentities + 1, source.SettledIdentityCount);

        // During the capture the partition keeps settling, and its watermark moves 20 s: the old identities
        // leave the window of the live slice, and each settlement would run one step of the sweep. A sweep
        // that ran here would remove the old identities from the shards the capture did not read yet, and the
        // section would pair the watermark 50,001 with a memory that lacks identities that watermark retains.
        using FirstWriteHook section = new(() =>
        {
            for (int i = 0; i < settledDuringCapture; i++)
                Settle(source, MakeIntent($"during/k{i:D6}", Ts(70_000 + i), Ts(70_001 + i)));
        });

        source.WritePartitionSection(section, Partition, static _ => true);

        Assert.True(source.GetLedgerWatermark(Partition) > Ts(70_000), "the settlements must have run during the capture");

        PreparedIntentStore.PartitionIntentSection decoded = PreparedIntentStore.DeserializePartitionIntents(section.ToArray());
        Assert.Equal(Ts(50_001), decoded.LedgerWatermark);

        HashSet<string> captured = new(StringComparer.Ordinal);
        foreach (PreparedIntentStore.SettledIdentityEntry entry in decoded.Settled!)
            Assert.True(captured.Add(entry.Key), $"{entry.Key} is in the section twice");

        for (int i = 0; i < oldIdentities; i++)
            Assert.Contains($"old/k{i:D6}", captured);

        Assert.Contains("mark/k", captured);

        // A replica seeded from the section judges a replayed prepare of an old leg as the exporter did at
        // that watermark: settled.
        PreparedIntentStore seeded = new();
        seeded.ReplacePartitionIntents(Partition, decoded, requireLedger: true, static _ => true);

        PreparedIntentApplyResult replay = seeded.Apply(new PrepareIntentCommand(MakeIntent("old/k004321", Ts(1_000), Ts(1_001))), Partition);
        Assert.True(replay.SettledReplay);
        Assert.Null(seeded.Get("old/k004321"));

        // The capture is over, so the sweep runs again: settlements that carry the watermark through two more
        // prune buckets take the old identities out of the source.
        for (int i = 0; i < 200; i++)
            Settle(source, MakeIntent($"after-a/k{i:D6}", Ts(70_400 + i), Ts(70_401 + i)));

        for (int i = 0; i < 200; i++)
            Settle(source, MakeIntent($"after-b/k{i:D6}", Ts(90_000 + i), Ts(90_001 + i)));

        Assert.Equal(1 + settledDuringCapture + 200 + 200, source.SettledIdentityCount);
    }

    [Fact]
    public void TheHeadOfEveryCapturedIdentity_IsInTheCapture()
    {
        PreparedIntentStore source = new();
        Settle(source, MakeIntent("first/k", Ts(1_000), Ts(1_001)));

        // A transaction prepares and settles between the two reads of the capture. The capture reads the
        // settled identities first and the heads second, so it holds this head and not this identity, and a
        // replica replays the transaction on top of that. In the other order the capture would hold the
        // identity and not the head: the replica would reject the replayed prepare as settled, the replayed
        // settle would find no intent, and the replica would never record the head.
        source.BetweenCaptureReadsForTesting = () =>
        {
            source.BetweenCaptureReadsForTesting = null;
            Settle(source, MakeIntent("gap/k", Ts(2_000), Ts(2_001)));
        };

        using MemoryStream section = new();
        source.WritePartitionSection(section, Partition, static _ => true);

        PreparedIntentStore.PartitionIntentSection decoded = PreparedIntentStore.DeserializePartitionIntents(section.ToArray());

        HashSet<string> heads = new(StringComparer.Ordinal);
        foreach ((string key, long _, KeyValueState _, HLCTimestamp _) in decoded.Ledger!)
            heads.Add(key);

        Assert.Contains("gap/k", heads);
        Assert.Contains(decoded.Settled!, entry => entry.Key == "first/k");

        foreach (PreparedIntentStore.SettledIdentityEntry entry in decoded.Settled!)
            Assert.Contains(entry.Key, heads);
    }

    [Fact]
    public void AnInstall_LeavesOutTheIdentitiesOutsideTheWindowOfItsWatermark()
    {
        // A section can hold identities its watermark no longer retains (an older build wrote every identity
        // it held). They are absent to every verdict, so the install does not spend memory on them.
        PreparedIntentStore.PartitionIntentSection section = new(
            Intents: [],
            Ledger: [("k/recent", 4, KeyValueState.Set, Ts(100_000))],
            LedgerWatermark: Ts(100_000),
            ReflectedThroughIndex: 0,
            Settled:
            [
                new PreparedIntentStore.SettledIdentityEntry(Ts(1_000), 1, "k/expired", Ts(1_001)),
                new PreparedIntentStore.SettledIdentityEntry(Ts(99_000), 1, "k/recent", Ts(100_000))
            ]);

        PreparedIntentStore imported = new();
        imported.ImportPartitionIntents(Partition, section, requireLedger: true);
        Assert.Equal(1, imported.SettledIdentityCount);

        PreparedIntentStore replaced = new();
        replaced.ReplacePartitionIntents(Partition, section, requireLedger: true, static _ => true);
        Assert.Equal(1, replaced.SettledIdentityCount);

        // The verdicts are those of a store that holds both entries: the recent leg is a settled replay, and
        // the expired one installs.
        Assert.True(replaced.Apply(new PrepareIntentCommand(MakeIntent("k/recent", Ts(99_000), Ts(100_000))), Partition).SettledReplay);
        Assert.False(replaced.Apply(new PrepareIntentCommand(MakeIntent("k/expired", Ts(1_000), Ts(1_001))), Partition).SettledReplay);
    }

    [Fact]
    public void ACaptureThatRunsWhileTransactionsSettle_KeepsEveryIdentityAndHeadFromBeforeIt()
    {
        const int before = 30_000;
        const long baseTime = 1_000_000;

        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => Partition);

        for (int i = 0; i < before; i++)
            Settle(store, MakeIntent($"before/k{i:D6}", Ts(baseTime, (uint)i), Ts(baseTime + 1 + i / 100)));

        // A writer settles new transactions for the whole test. Its commit times advance 1 ms a transaction,
        // so the slice's watermark crosses prune buckets (a sweep starts and runs) and stays inside the window
        // of the identities from before.
        using CancellationTokenSource stop = new();
        int settledByWriter = 0;
        Exception? writerFault = null;

        Thread writer = new(() =>
        {
            try
            {
                for (int i = 0; !stop.IsCancellationRequested && i < 40_000; i++)
                {
                    long time = baseTime + 1_000 + i;
                    Settle(store, MakeIntent($"writer/k{i:D6}", Ts(time), Ts(time + 1)));
                    Volatile.Write(ref settledByWriter, i + 1);
                }
            }
            catch (Exception ex)
            {
                writerFault = ex;
            }
        })
        { IsBackground = true, Name = "ledger-capture-writer" };

        writer.Start();

        // Each capture runs against a memory that grows and splits under it.
        int captures = 0;
        while (captures < 6 || (Volatile.Read(ref settledByWriter) < 5_000 && writer.IsAlive))
        {
            Assert.True(store.PersistSnapshot(Partition));
            captures++;
        }

        int settledBeforeLastCapture = Volatile.Read(ref settledByWriter);
        Assert.True(store.PersistSnapshot(Partition));

        stop.Cancel();
        writer.Join();
        Assert.Null(writerFault);

        PreparedIntentStore restarted = new(dir, "rev", null);
        restarted.AttachPartitionResolver(_ => Partition);

        HashSet<string> restored = new(StringComparer.Ordinal);
        foreach (PreparedIntentStore.SettledIdentityEntry entry in restarted.SnapshotSettledIdentities(Partition))
            Assert.True(restored.Add(entry.Key), $"{entry.Key} was restored twice");

        // Present for the whole of the last capture: the identities from before, and those the writer settled
        // before that capture started.
        for (int i = 0; i < before; i++)
        {
            string key = $"before/k{i:D6}";
            Assert.Contains(key, restored);
            Assert.True(restarted.TryGetLedgerHead(Partition, key, out _, out _, out _), $"the head of {key} is missing");
        }

        for (int i = 0; i < settledBeforeLastCapture; i++)
        {
            string key = $"writer/k{i:D6}";
            Assert.Contains(key, restored);
            Assert.True(restarted.TryGetLedgerHead(Partition, key, out _, out _, out _), $"the head of {key} is missing");
        }

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{captures + 1} captures while {Volatile.Read(ref settledByWriter)} transactions settled; " +
            $"{restored.Count} identities restored; the last capture held the gate for at most {store.LastLedgerCaptureLongestGateHold.TotalMilliseconds:F3} ms in one step");
    }
}

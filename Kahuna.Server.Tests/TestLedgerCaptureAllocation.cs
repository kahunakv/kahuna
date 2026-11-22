using System.Diagnostics;

using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.Tests;

/// <summary>
/// The checkpoint capture of a ledger slice with one million settled identities. The capture copied the whole
/// settled-identity memory into one list under the apply gate: one allocation of about 48 MiB, and an apply
/// stream that waited for the whole copy. A node at its heap limit died in that allocation. The capture now
/// reads the memory one small chunk at a time and writes each chunk to the file before it reads the next.
///
/// <para>The allocation events are process-wide, so the test runs in the exclusive collection, and the probe
/// reads only the events of the thread that runs the capture (see <see cref="LargeAllocationProbe"/>).</para>
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestLedgerCaptureAllocation : IDisposable
{
    private const int Partition = 3;

    private const int SettledIdentities = 1_000_000;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-ledger-capture-" + Guid.NewGuid().ToString("N"));

    public TestLedgerCaptureAllocation() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    /// <summary>A store whose slice holds <paramref name="count"/> settled identities with 30-character keys.
    /// The removal of an absent intent records the identity, so one apply makes one entry.</summary>
    private PreparedIntentStore StoreWithSettledIdentities(int count)
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => Partition);

        for (int i = 0; i < count; i++)
            store.Apply(new RemoveIntentCommand(Ts(1_000 + i), 1, $"ranged1/accounts/k{i:D12}"), Partition);

        return store;
    }

    [Fact]
    public void Capture_AtOneMillionSettledIdentities_MakesNoAllocationAboveOneMebibyte()
    {
        PreparedIntentStore store = StoreWithSettledIdentities(SettledIdentities);

        // The longest time another thread waits for the apply gate while the capture runs: what an apply of the
        // partition sees.
        long longestGateWaitTicks = 0;
        long gateProbes = 0;
        using CancellationTokenSource stopProbe = new();
        Thread probe = new(() =>
        {
            while (!stopProbe.IsCancellationRequested)
            {
                long start = Stopwatch.GetTimestamp();
                store.GetLedgerWatermark(Partition);
                long waited = Stopwatch.GetTimestamp() - start;

                if (waited > longestGateWaitTicks)
                    longestGateWaitTicks = waited;

                gateProbes++;
                Thread.SpinWait(200);
            }
        })
        { IsBackground = true, Name = "ledger-capture-gate-probe" };

        long captureTicks = 0;
        bool persisted = false;

        probe.Start();

        IReadOnlyList<AllocationTick> ticks = LargeAllocationProbe.TicksOf(() =>
        {
            long start = Stopwatch.GetTimestamp();
            persisted = store.PersistSnapshot(Partition);
            captureTicks = Stopwatch.GetTimestamp() - start;
        });

        stopProbe.Cancel();
        probe.Join();

        Assert.True(persisted);

        AllocationTick largest = LargeAllocationProbe.Largest(ticks);

        long fileBytes = new FileInfo(Directory.GetFiles(dir, "*_p*.snapshot").Single()).Length;

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"capture of {SettledIdentities} settled identities: {Stopwatch.GetElapsedTime(0, captureTicks).TotalMilliseconds:F1} ms, " +
            $"file {fileBytes} B, longest wait for the apply gate {Stopwatch.GetElapsedTime(0, longestGateWaitTicks).TotalMilliseconds:F2} ms over {gateProbes} probes, " +
            $"longest hold of the gate by one step of the capture {store.LastLedgerCaptureLongestGateHold.TotalMilliseconds:F3} ms, " +
            $"largest single allocation {largest.ObjectSize} B ({largest.TypeName}), {ticks.Count} allocation ticks");

        Assert.True(
            largest.ObjectSize <= LargeAllocationProbe.OneMebibyte,
            $"the capture allocated one object of {largest.ObjectSize} B ({largest.TypeName})");

        // The file holds every identity: a restarted node rejects the replayed prepare of each one.
        PreparedIntentStore restarted = new(dir, "rev", null);
        restarted.AttachPartitionResolver(_ => Partition);
        Assert.Equal(SettledIdentities, restarted.SnapshotSettledIdentities(Partition).Count);
    }
}

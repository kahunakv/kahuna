using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;

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
/// <para>The allocation events are process-wide, so the test runs in the exclusive collection, and it reads
/// only the events of its own thread.</para>
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestLedgerCaptureAllocation : IDisposable
{
    private const int Partition = 3;

    private const int SettledIdentities = 1_000_000;

    private const long OneMebibyte = 1024 * 1024;

    // Sizes no other allocation of the process has, so the two marker events are unambiguous.
    private const int StartMarkerBytes = 2 * 1024 * 1024 + 1_031;

    private const int EndMarkerBytes = 2 * 1024 * 1024 + 2_063;

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

        using AllocationTickListener listener = new();

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

        // The capture runs on a thread of its own so that its allocation events carry one thread id.
        Thread capture = new(() =>
        {
            GC.KeepAlive(new byte[StartMarkerBytes]);

            long start = Stopwatch.GetTimestamp();
            persisted = store.PersistSnapshot(Partition);
            captureTicks = Stopwatch.GetTimestamp() - start;

            GC.KeepAlive(new byte[EndMarkerBytes]);
        })
        { Name = "ledger-capture" };

        probe.Start();
        capture.Start();
        capture.Join();
        stopProbe.Cancel();
        probe.Join();

        Assert.True(persisted);

        IReadOnlyList<AllocationTick> ticks = listener.TicksBetween(StartMarkerBytes, EndMarkerBytes, TimeSpan.FromSeconds(30));

        AllocationTick largest = default;
        foreach (AllocationTick tick in ticks)
        {
            if (tick.ObjectSize > largest.ObjectSize)
                largest = tick;
        }

        long fileBytes = new FileInfo(Directory.GetFiles(dir, "*_p*.snapshot").Single()).Length;

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"capture of {SettledIdentities} settled identities: {Stopwatch.GetElapsedTime(0, captureTicks).TotalMilliseconds:F1} ms, " +
            $"file {fileBytes} B, longest wait for the apply gate {Stopwatch.GetElapsedTime(0, longestGateWaitTicks).TotalMilliseconds:F2} ms over {gateProbes} probes, " +
            $"longest hold of the gate by one step of the capture {store.LastLedgerCaptureLongestGateHold.TotalMilliseconds:F3} ms, " +
            $"largest single allocation {largest.ObjectSize} B ({largest.TypeName}), {ticks.Count} allocation ticks");

        Assert.True(
            largest.ObjectSize <= OneMebibyte,
            $"the capture allocated one object of {largest.ObjectSize} B ({largest.TypeName})");

        // The file holds every identity: a restarted node rejects the replayed prepare of each one.
        PreparedIntentStore restarted = new(dir, "rev", null);
        restarted.AttachPartitionResolver(_ => Partition);
        Assert.Equal(SettledIdentities, restarted.SnapshotSettledIdentities(Partition).Count);
    }

    private readonly record struct AllocationTick(long ThreadId, long ObjectSize, string TypeName);

    /// <summary>
    /// Collects the runtime's allocation tick events. The runtime raises one for each object above about
    /// 100 KB, with the size and the type of that object, so no allocation above 1 MiB can pass without one.
    /// </summary>
    private sealed class AllocationTickListener : EventListener
    {
        private const EventKeywords GarbageCollection = (EventKeywords)0x1;

        private readonly ConcurrentQueue<AllocationTick> ticks = new();

        protected override void OnEventSourceCreated(EventSource source)
        {
            if (source.Name == "Microsoft-Windows-DotNETRuntime")
                EnableEvents(source, EventLevel.Verbose, GarbageCollection);
        }

        protected override void OnEventWritten(EventWrittenEventArgs sample)
        {
            if (sample.EventName is null || !sample.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal)
                || sample.Payload is null || sample.PayloadNames is null)
                return;

            int type = sample.PayloadNames.IndexOf("TypeName");
            int size = sample.PayloadNames.IndexOf("ObjectSize");
            if (type < 0 || size < 0)
                return;

            ticks.Enqueue(new AllocationTick(
                sample.OSThreadId, Convert.ToInt64(sample.Payload[size]), sample.Payload[type]?.ToString() ?? "unknown"));
        }

        /// <summary>The ticks of the thread that allocated the two markers, between the markers. The events
        /// arrive on a dispatch thread after a delay, so this waits for the end marker.</summary>
        public IReadOnlyList<AllocationTick> TicksBetween(int startMarkerBytes, int endMarkerBytes, TimeSpan timeout)
        {
            long deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);

            while (true)
            {
                AllocationTick[] seen = [.. ticks];

                int start = Array.FindIndex(seen, tick => IsMarker(tick, startMarkerBytes));
                int end = start < 0 ? -1 : Array.FindIndex(seen, start + 1, tick => IsMarker(tick, endMarkerBytes) && tick.ThreadId == seen[start].ThreadId);

                if (end >= 0)
                {
                    List<AllocationTick> between = [];
                    for (int i = start + 1; i < end; i++)
                    {
                        if (seen[i].ThreadId == seen[start].ThreadId)
                            between.Add(seen[i]);
                    }

                    return between;
                }

                Assert.True(Stopwatch.GetTimestamp() < deadline, "the allocation events of the two markers did not arrive");
                Thread.Sleep(50);
            }
        }

        // The object size includes the array header.
        private static bool IsMarker(AllocationTick tick, int markerBytes) =>
            tick.ObjectSize >= markerBytes && tick.ObjectSize <= markerBytes + 64;
    }
}

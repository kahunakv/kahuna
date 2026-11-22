using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Runtime.ExceptionServices;

namespace Kahuna.Server.Tests;

/// <summary>One allocation tick of the runtime: the thread, and the size and type of the object that raised it.</summary>
internal readonly record struct AllocationTick(long ThreadId, long ObjectSize, string TypeName);

/// <summary>
/// Finds the large single allocations of a piece of work. The runtime raises one allocation tick for each
/// object above about 100 KB, with the size and the type of that object, so no allocation above 1 MiB can pass
/// without one.
///
/// <para>The events are process-wide. The probe runs the work on a thread of its own between two marker
/// allocations and keeps the events of that thread only, so a test that uses it does not need the process to
/// be idle. Work that hops threads is outside what the probe sees.</para>
/// </summary>
internal static class LargeAllocationProbe
{
    public const long OneMebibyte = 1024 * 1024;

    // Sizes no other allocation of the process has, so the two marker events are unambiguous.
    private const int StartMarkerBytes = 2 * 1024 * 1024 + 1_031;

    private const int EndMarkerBytes = 2 * 1024 * 1024 + 2_063;

    /// <summary>Runs <paramref name="work"/> on a new thread and returns every allocation tick of that thread
    /// while the work ran. An exception of the work is thrown again here.</summary>
    public static IReadOnlyList<AllocationTick> TicksOf(Action work)
    {
        using Listener listener = new();

        ExceptionDispatchInfo? fault = null;

        Thread thread = new(() =>
        {
            GC.KeepAlive(new byte[StartMarkerBytes]);

            try
            {
                work();
            }
            catch (Exception ex)
            {
                fault = ExceptionDispatchInfo.Capture(ex);
            }

            GC.KeepAlive(new byte[EndMarkerBytes]);
        })
        { Name = "large-allocation-probe" };

        thread.Start();
        thread.Join();

        fault?.Throw();

        return listener.TicksBetweenMarkers(TimeSpan.FromSeconds(30));
    }

    /// <summary>The largest tick of <paramref name="ticks"/>, or a tick of size zero when there is none.</summary>
    public static AllocationTick Largest(IReadOnlyList<AllocationTick> ticks)
    {
        AllocationTick largest = default;

        foreach (AllocationTick tick in ticks)
        {
            if (tick.ObjectSize > largest.ObjectSize)
                largest = tick;
        }

        return largest;
    }

    /// <summary>The ticks above <paramref name="thresholdBytes"/> as one line for each type: how many, the
    /// largest and the total. Empty when there is none.</summary>
    public static string Describe(IReadOnlyList<AllocationTick> ticks, long thresholdBytes = OneMebibyte)
    {
        Dictionary<string, (int Count, long Largest, long Total)> byType = new(StringComparer.Ordinal);

        foreach (AllocationTick tick in ticks)
        {
            if (tick.ObjectSize <= thresholdBytes)
                continue;

            (int count, long largest, long total) = byType.GetValueOrDefault(tick.TypeName);
            byType[tick.TypeName] = (count + 1, Math.Max(largest, tick.ObjectSize), total + tick.ObjectSize);
        }

        return string.Join("; ", byType
            .OrderByDescending(pair => pair.Value.Largest)
            .Select(pair => $"{pair.Value.Count} x {pair.Key}, largest {pair.Value.Largest} B, total {pair.Value.Total} B"));
    }

    private sealed class Listener : EventListener
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
        public IReadOnlyList<AllocationTick> TicksBetweenMarkers(TimeSpan timeout)
        {
            long deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);

            while (true)
            {
                AllocationTick[] seen = [.. ticks];

                int start = Array.FindIndex(seen, tick => IsMarker(tick, StartMarkerBytes));
                int end = start < 0 ? -1 : Array.FindIndex(seen, start + 1, tick => IsMarker(tick, EndMarkerBytes) && tick.ThreadId == seen[start].ThreadId);

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

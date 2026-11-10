namespace Kahuna.Server.Tests;

/// <summary>
/// The process-wide live heap, read as the bytes that survived a forced full blocking collection.
///
/// <para><see cref="GC.GetTotalMemory(bool)"/> is not that number: it is read after the collection ends, so it
/// also counts what other threads allocated in between and the allocation space the collector handed out, and
/// two reads of one unchanged set of live objects differ by tens of megabytes. The collector's own record of
/// the collection is exact for the moment the collection ended, so the difference between two of these reads is
/// the difference in live objects and nothing else.</para>
///
/// <para>The number is process-wide. A test that uses it must be in the collection that runs on its own.</para>
/// </summary>
internal static class LiveHeapMeasurement
{
    /// <summary>Forces a full blocking collection, lets pending finalizers release what they hold, collects again
    /// and returns the bytes that survived.</summary>
    public static long Measure()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        return Collect();
    }

    /// <summary>Forces one full blocking collection and returns the bytes that survived it. Unlike
    /// <see cref="Measure"/> this never waits for finalizers, so a sampling thread can call it while the measured
    /// code runs.</summary>
    public static long Collect()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true);

        GCMemoryInfo info = GC.GetGCMemoryInfo(GCKind.FullBlocking);

        long live = 0;
        foreach (GCGenerationInfo generation in info.GenerationInfo)
            live += generation.SizeAfterBytes - generation.FragmentationAfterBytes;

        return live;
    }
}

/// <summary>
/// Samples <see cref="LiveHeapMeasurement.Collect"/> on a thread of its own from construction until
/// <see cref="Stop"/>, and keeps the largest value. A sample can fall between two moments of higher use, so the
/// peak can only under-read the true one.
/// </summary>
internal sealed class LiveHeapPeakSampler : IDisposable
{
    private readonly CancellationTokenSource stop = new();

    private readonly Thread thread;

    // Written by the sampling thread only, and read only after that thread was joined.
    private long peak;

    public LiveHeapPeakSampler(string name)
    {
        thread = new(Sample) { IsBackground = true, Name = name };
        thread.Start();
    }

    private void Sample()
    {
        while (!stop.IsCancellationRequested)
        {
            long live = LiveHeapMeasurement.Collect();
            if (live > peak)
                peak = live;

            // Paced so the measured code makes progress between collections.
            Thread.Sleep(20);
        }
    }

    /// <summary>Stops the sampling thread and returns the largest live heap it saw.</summary>
    public long Stop()
    {
        stop.Cancel();
        thread.Join();
        return peak;
    }

    public void Dispose()
    {
        Stop();
        stop.Dispose();
    }
}

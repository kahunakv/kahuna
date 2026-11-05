using Kahuna.Server.Diagnostics;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Tests;

/// <summary>
/// The out-of-memory restart breaker: a node whose last starts all ended in an out-of-memory fail-fast early
/// in recovery, under the heap limit it still runs with, trips at start and reports the reason instead of
/// crash-looping. Decided from the exit ledger the fail-fast policy appends to; judged here through the pure
/// ledger verdict and the recorder's line format, never through a real out-of-memory.
/// </summary>
public sealed class TestOutOfMemoryRestartBreaker : IDisposable
{
    private readonly string tempDir = Path.Combine(Path.GetTempPath(), "kahuna-oom-breaker-" + Guid.NewGuid().ToString("N"));

    private const long HeapLimit = 2_458L * 1024 * 1024;

    public TestOutOfMemoryRestartBreaker() => Directory.CreateDirectory(tempDir);

    public void Dispose()
    {
        try { Directory.Delete(tempDir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private string LedgerPath => Path.Combine(tempDir, OutOfMemoryRestartBreaker.LedgerFileName);

    private static OutOfMemoryExitRecord Exit(DateTimeOffset at, TimeSpan uptime, long heapLimit = HeapLimit, string site = "KeyValueRestorer.Restore") =>
        new(at, uptime, heapLimit, site);

    private void WriteLedger(params OutOfMemoryExitRecord[] records) =>
        File.WriteAllLines(LedgerPath, Array.ConvertAll(records, OutOfMemoryRestartBreaker.FormatLine));

    [Fact]
    public void TrailingRun_CountsRecoveryExitsUnderTheSameHeapLimit_AndStopsAtTheFirstThatIsNot()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;

        // Two steady-state exits long ago, one under another heap limit, then three recovery exits in a row.
        List<OutOfMemoryExitRecord> records =
        [
            Exit(now.AddMinutes(-50), TimeSpan.FromHours(2)),
            Exit(now.AddMinutes(-40), TimeSpan.FromSeconds(75), heapLimit: HeapLimit / 2),
            Exit(now.AddMinutes(-30), TimeSpan.FromSeconds(75)),
            Exit(now.AddMinutes(-20), TimeSpan.FromSeconds(83)),
            Exit(now.AddMinutes(-10), TimeSpan.FromSeconds(136)),
        ];

        Assert.Equal(3, OutOfMemoryRestartBreaker.CountTrailingRecoveryExits(records, HeapLimit));

        // A different heap limit now (the operator raised the memory limit): nothing in the ledger applies.
        Assert.Equal(0, OutOfMemoryRestartBreaker.CountTrailingRecoveryExits(records, HeapLimit * 2));

        // The latest exit happened late in a long-lived process: not a crash loop in recovery.
        records.Add(Exit(now.AddMinutes(-1), TimeSpan.FromHours(1)));
        Assert.Equal(0, OutOfMemoryRestartBreaker.CountTrailingRecoveryExits(records, HeapLimit));
    }

    [Fact]
    public void Window_DropsExitsOlderThanAnHour()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;

        List<OutOfMemoryExitRecord> recent = OutOfMemoryRestartBreaker.WithinWindow(
        [
            Exit(now.AddHours(-3), TimeSpan.FromSeconds(60)),
            Exit(now.AddMinutes(-59), TimeSpan.FromSeconds(60)),
            Exit(now.AddMinutes(-1), TimeSpan.FromSeconds(60)),
        ], now);

        Assert.Equal(2, recent.Count);
        Assert.Equal(now.AddMinutes(-59).ToUnixTimeMilliseconds(), recent[0].At.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void Line_RoundTrips_AndAMalformedLineIsSkipped()
    {
        OutOfMemoryExitRecord record = Exit(DateTimeOffset.FromUnixTimeMilliseconds(1_793_822_855_123), TimeSpan.FromMilliseconds(75_250), site: "PreparedIntentStore.BuildLedger");

        string line = OutOfMemoryRestartBreaker.FormatLine(record);
        Assert.Equal("1793822855123 75250 2577399808 PreparedIntentStore.BuildLedger", line);

        Assert.True(OutOfMemoryRestartBreaker.TryParseLine(line, out OutOfMemoryExitRecord parsed));
        Assert.Equal(record, parsed);

        Assert.False(OutOfMemoryRestartBreaker.TryParseLine("", out _));
        Assert.False(OutOfMemoryRestartBreaker.TryParseLine("not a ledger line", out _));
        Assert.False(OutOfMemoryRestartBreaker.TryParseLine("-5 10 100 site", out _));
    }

    [Fact]
    public void Verdict_TripsAtTheThreshold_AndNotBelowIt_AndNeverWhenDisabled()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        WriteLedger(
            Exit(now.AddMinutes(-20), TimeSpan.FromSeconds(75)),
            Exit(now.AddMinutes(-12), TimeSpan.FromSeconds(83)),
            Exit(now.AddMinutes(-5), TimeSpan.FromSeconds(136)));

        OutOfMemoryRestartBreaker.LedgerVerdict below = OutOfMemoryRestartBreaker.EvaluateLedger(LedgerPath, threshold: 4, HeapLimit, now, logger: null);
        Assert.Equal(3, below.Run);
        Assert.Null(below.Reason);

        OutOfMemoryRestartBreaker.LedgerVerdict tripped = OutOfMemoryRestartBreaker.EvaluateLedger(LedgerPath, threshold: 3, HeapLimit, now, logger: null);
        Assert.Equal(3, tripped.Run);
        Assert.NotNull(tripped.Reason);
        Assert.Contains("3 consecutive starts", tripped.Reason);
        Assert.Contains("KeyValueRestorer.Restore", tripped.Reason);

        OutOfMemoryRestartBreaker.LedgerVerdict disabled = OutOfMemoryRestartBreaker.EvaluateLedger(LedgerPath, threshold: 0, HeapLimit, now, logger: null);
        Assert.Equal(3, disabled.Run);
        Assert.Null(disabled.Reason);

        OutOfMemoryRestartBreaker.LedgerVerdict absent = OutOfMemoryRestartBreaker.EvaluateLedger(Path.Combine(tempDir, "missing.log"), threshold: 1, HeapLimit, now, logger: null);
        Assert.Equal(0, absent.Run);
        Assert.Null(absent.Reason);
    }

    [Fact]
    public void Verdict_TrimsTheLedgerToTheWindow()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        WriteLedger(
            Exit(now.AddHours(-5), TimeSpan.FromSeconds(75)),
            Exit(now.AddHours(-4), TimeSpan.FromSeconds(75)),
            Exit(now.AddMinutes(-3), TimeSpan.FromSeconds(90)));

        OutOfMemoryRestartBreaker.LedgerVerdict verdict = OutOfMemoryRestartBreaker.EvaluateLedger(LedgerPath, threshold: 3, HeapLimit, now, logger: null);

        Assert.Equal(1, verdict.Run);
        Assert.Null(verdict.Reason);
        Assert.Single(File.ReadAllLines(LedgerPath));
    }

    /// <summary>The recorder appends a line the next start parses, without touching what the file already held.</summary>
    [Fact]
    public void Recorder_AppendsAParseableLine()
    {
        DateTimeOffset before = DateTimeOffset.UtcNow;
        File.WriteAllText(LedgerPath, OutOfMemoryRestartBreaker.FormatLine(Exit(before.AddMinutes(-30), TimeSpan.FromSeconds(60))) + "\n");

        using (OutOfMemoryExitRecorder recorder = new(LedgerPath, HeapLimit))
        {
            recorder.RecordExit("DurableMaintenanceService.CollectDurableTransactionRecords");
            recorder.RecordExit(new string('x', 2_000));
        }

        List<OutOfMemoryExitRecord> records = OutOfMemoryRestartBreaker.ReadLedger(LedgerPath, logger: null);

        Assert.Equal(3, records.Count);
        Assert.Equal(HeapLimit, records[1].HeapLimitBytes);
        Assert.Equal("DurableMaintenanceService.CollectDurableTransactionRecords", records[1].Site);
        Assert.InRange(records[1].At, DateTimeOffset.FromUnixTimeMilliseconds(before.ToUnixTimeMilliseconds()), DateTimeOffset.UtcNow.AddSeconds(1));
        Assert.True(records[1].Uptime < TimeSpan.FromMinutes(1));

        // An over-long site is truncated to the line, never dropped.
        Assert.StartsWith("xxxx", records[2].Site);
        Assert.True(records[2].Site.Length < 600);

        // All three count as recovery exits under this heap limit.
        Assert.Equal(3, OutOfMemoryRestartBreaker.CountTrailingRecoveryExits(records, HeapLimit));
    }

    /// <summary>An install's boundary raises the provider's cached persisted floor, never lowers it.</summary>
    [Fact]
    public void DurabilityProvider_NoteInstalledThrough_RaisesTheCachedFloor()
    {
        MemoryPersistenceBackend backend = new();
        PartitionDurabilityTracker tracker = new();
        KahunaDurabilityProvider provider = new(tracker, backend);

        Assert.Equal(-1, provider.GetDurablyAppliedIndex(2));

        provider.NoteInstalledThrough(2, 4_200);
        Assert.Equal(4_200, provider.GetDurablyAppliedIndex(2));

        provider.NoteInstalledThrough(2, 100);
        Assert.Equal(4_200, provider.GetDurablyAppliedIndex(2));
    }
}

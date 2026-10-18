using System.Diagnostics;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Paces a sequence of reads or writes to a byte budget over its whole run, across every file it touches, so
/// a backup step that walks many files holds one rate instead of one rate per file. A budget of zero or less
/// never waits. Not thread-safe: one pacer serves one sequential pass.
/// </summary>
internal sealed class ThroughputPacer
{
    private readonly long _bytesPerSec;
    private readonly long _start = Stopwatch.GetTimestamp();
    private long _bytes;

    internal ThroughputPacer(long bytesPerSec) => _bytesPerSec = bytesPerSec;

    internal bool IsUnlimited => _bytesPerSec <= 0;

    /// <summary>Counts <paramref name="bytes"/> and waits until the run is back at or under the budget.</summary>
    internal ValueTask PaceAsync(int bytes, CancellationToken ct)
    {
        if (_bytesPerSec <= 0)
            return ValueTask.CompletedTask;

        _bytes += bytes;
        double delay = BackupService.ThrottleDelaySeconds(_bytes, _bytesPerSec, Stopwatch.GetElapsedTime(_start).TotalSeconds);
        return delay > 0.001
            ? new ValueTask(Task.Delay(TimeSpan.FromSeconds(delay), ct))
            : ValueTask.CompletedTask;
    }
}

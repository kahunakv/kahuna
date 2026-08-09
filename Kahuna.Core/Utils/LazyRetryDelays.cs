
using Polly.Contrib.WaitAndRetry;

namespace Kahuna.Utils;

/// <summary>
/// Decorrelated-jitter retry schedule that is materialized only when the first delay is
/// actually consumed. Retry loops overwhelmingly succeed on the first attempt; constructing
/// this struct costs nothing, so those calls never allocate the jitter sequence at all.
/// Use as a local variable next to a counted retry loop and call <see cref="TryNext"/> only
/// on the retry branch.
/// </summary>
public struct LazyRetryDelays
{
    private readonly TimeSpan medianFirstRetryDelay;

    private readonly int retryCount;

    private IEnumerator<TimeSpan>? enumerator;

    public LazyRetryDelays(TimeSpan medianFirstRetryDelay, int retryCount)
    {
        this.medianFirstRetryDelay = medianFirstRetryDelay;
        this.retryCount = retryCount;
    }

    /// <summary>
    /// Returns the next delay in the schedule, building the jittered sequence on first use.
    /// Returns false once the schedule is exhausted.
    /// </summary>
    public bool TryNext(out TimeSpan delay)
    {
        enumerator ??= Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay, retryCount).GetEnumerator();

        if (enumerator.MoveNext())
        {
            delay = enumerator.Current;
            return true;
        }

        delay = default;
        return false;
    }
}

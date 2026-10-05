/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.RateLimiting;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// The lease a Kahuna-backed limiter hands out.
///
/// A refused lease carries <see cref="MetadataName.RetryAfter"/> when the cluster could tell when the
/// budget frees up, which is what an HTTP 429 handler reads to fill the <c>Retry-After</c> header.
/// A granted lease of a concurrency limiter runs its release when it is disposed. Every other lease
/// has nothing to give back: a window or a bucket refills with time, not with disposal.
/// </summary>
internal sealed class KahunaRateLimitLease : RateLimitLease
{
    private static readonly string[] RetryAfterNames = [MetadataName.RetryAfter.Name];

    /// <summary>A granted lease with nothing to release, shared because it holds no state.</summary>
    public static readonly KahunaRateLimitLease Granted = new(true, null, null);

    /// <summary>A refused lease that cannot say when to retry, shared because it holds no state.</summary>
    public static readonly KahunaRateLimitLease Refused = new(false, null, null);

    private readonly TimeSpan? retryAfter;

    private Action? release;

    public KahunaRateLimitLease(bool isAcquired, TimeSpan? retryAfter, Action? release)
    {
        IsAcquired = isAcquired;
        this.retryAfter = retryAfter;
        this.release = release;
    }

    public override bool IsAcquired { get; }

    public override IEnumerable<string> MetadataNames => retryAfter.HasValue ? RetryAfterNames : [];

    public override bool TryGetMetadata(string metadataName, out object? metadata)
    {
        if (retryAfter.HasValue && string.Equals(metadataName, MetadataName.RetryAfter.Name, StringComparison.Ordinal))
        {
            metadata = retryAfter.Value;
            return true;
        }

        metadata = null;
        return false;
    }

    /// <summary>
    /// Runs the release once, however many times the lease is disposed and from whichever thread.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        Interlocked.Exchange(ref release, null)?.Invoke();
    }
}

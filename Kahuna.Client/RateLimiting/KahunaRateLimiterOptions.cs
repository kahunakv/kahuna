/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.RateLimiting;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// What a Kahuna-backed limiter answers when it cannot reach a decision, because the cluster is
/// unreachable or every retry of a contended counter aborted.
/// </summary>
public enum KahunaRateLimiterFailureMode
{
    /// <summary>
    /// Surface the failure to the caller as an exception. Nothing is admitted or refused silently.
    /// </summary>
    Throw,

    /// <summary>
    /// Admit the request. The limit stops protecting the application while the cluster is down, but
    /// the application keeps serving. The admitted lease holds no permit on the cluster.
    /// </summary>
    Allow,

    /// <summary>
    /// Refuse the request, as if the limit were spent.
    /// </summary>
    Deny
}

/// <summary>
/// Settings every Kahuna-backed limiter shares.
///
/// The counter lives in Kahuna, so every process that names the same <see cref="Key"/> with the same
/// limits spends one budget between them. Two processes that name one key with different limits do
/// not agree on anything: each one judges the shared counter by its own numbers.
/// </summary>
public abstract class KahunaRateLimiterOptions
{
    /// <summary>
    /// The Kahuna key that holds this limiter's state. Every limiter that must share a budget names
    /// the same key, and every limiter that must not share one names a different key.
    /// </summary>
    public string Key { get; set; } = "";

    /// <summary>
    /// Where the state lives. <see cref="KeyValueDurability.Ephemeral"/> keeps it in memory on the
    /// cluster: it is far cheaper, and a restart of the whole cluster only resets the budgets.
    /// <see cref="KeyValueDurability.Persistent"/> replicates and persists every admission.
    /// </summary>
    public KeyValueDurability Durability { get; set; } = KeyValueDurability.Ephemeral;

    /// <summary>
    /// How many permits may wait in this process for the budget to free up. Zero refuses at once.
    /// The queue is local to the process: each process waits on its own queue for the shared budget.
    /// </summary>
    public int QueueLimit { get; set; }

    /// <summary>
    /// Which waiter is served first. <see cref="QueueProcessingOrder.NewestFirst"/> also evicts the
    /// oldest waiters, with a refused lease, to make room for a new one when the queue is full.
    /// </summary>
    public QueueProcessingOrder QueueProcessingOrder { get; set; } = QueueProcessingOrder.OldestFirst;

    /// <summary>
    /// What the limiter answers when it cannot reach a decision.
    /// </summary>
    public KahunaRateLimiterFailureMode FailureMode { get; set; } = KahunaRateLimiterFailureMode.Throw;

    /// <summary>
    /// How many times a decision that aborted on a contended counter is attempted again before
    /// <see cref="FailureMode"/> applies. An aborted attempt changed nothing, so a retry cannot
    /// spend a permit twice.
    /// </summary>
    public int MaxRetries { get; set; } = 8;

    internal void ValidateCommon()
    {
        if (string.IsNullOrEmpty(Key))
            throw new ArgumentException("A Kahuna rate limiter needs a non-empty Key.", nameof(Key));

        if (QueueLimit < 0)
            throw new ArgumentException("QueueLimit must not be negative.", nameof(QueueLimit));

        if (MaxRetries < 0)
            throw new ArgumentException("MaxRetries must not be negative.", nameof(MaxRetries));
    }
}

/// <summary>
/// Settings of <see cref="KahunaFixedWindowRateLimiter"/>.
/// </summary>
public sealed class KahunaFixedWindowRateLimiterOptions : KahunaRateLimiterOptions
{
    /// <summary>
    /// How many permits one window holds.
    /// </summary>
    public int PermitLimit { get; set; }

    /// <summary>
    /// The length of one window. Windows are aligned to the cluster clock, so every process that
    /// shares the key agrees on where a window starts. Whole milliseconds only.
    /// </summary>
    public TimeSpan Window { get; set; }

    internal void Validate()
    {
        ValidateCommon();

        if (PermitLimit <= 0)
            throw new ArgumentException("PermitLimit must be greater than zero.", nameof(PermitLimit));

        if (Window.TotalMilliseconds < 1)
            throw new ArgumentException("Window must be at least one millisecond.", nameof(Window));
    }
}

/// <summary>
/// Settings of <see cref="KahunaSlidingWindowRateLimiter"/>.
/// </summary>
public sealed class KahunaSlidingWindowRateLimiterOptions : KahunaRateLimiterOptions
{
    /// <summary>
    /// The largest accepted <see cref="SegmentsPerWindow"/>. Every segment is a field of one value
    /// that each decision reads, rewrites and loops over.
    /// </summary>
    public const int MaxSegmentsPerWindow = 1000;

    /// <summary>
    /// How many permits the window holds at any moment.
    /// </summary>
    public int PermitLimit { get; set; }

    /// <summary>
    /// The length of the window. It is cut into <see cref="SegmentsPerWindow"/> segments of whole
    /// milliseconds, so the effective window is rounded down to a multiple of the segment count.
    /// </summary>
    public TimeSpan Window { get; set; }

    /// <summary>
    /// How many segments the window is cut into. The permits of a segment come back when that
    /// segment slides out of the window. More segments track the window more closely and make each
    /// decision cost more, because the whole window is one value on the cluster.
    /// </summary>
    public int SegmentsPerWindow { get; set; }

    internal void Validate()
    {
        ValidateCommon();

        if (PermitLimit <= 0)
            throw new ArgumentException("PermitLimit must be greater than zero.", nameof(PermitLimit));

        if (SegmentsPerWindow <= 0 || SegmentsPerWindow > MaxSegmentsPerWindow)
            throw new ArgumentException($"SegmentsPerWindow must be between 1 and {MaxSegmentsPerWindow}.", nameof(SegmentsPerWindow));

        if ((long)Window.TotalMilliseconds < SegmentsPerWindow)
            throw new ArgumentException("Window must hold at least one millisecond per segment.", nameof(Window));
    }
}

/// <summary>
/// Settings of <see cref="KahunaTokenBucketRateLimiter"/>.
/// </summary>
public sealed class KahunaTokenBucketRateLimiterOptions : KahunaRateLimiterOptions
{
    /// <summary>
    /// How many tokens the bucket holds when it is full. A new bucket starts full.
    /// </summary>
    public int TokenLimit { get; set; }

    /// <summary>
    /// How often the bucket gains <see cref="TokensPerPeriod"/> tokens. Whole milliseconds only.
    /// Replenishment is computed from the cluster clock when a request arrives, so no process runs a
    /// timer for it.
    /// </summary>
    public TimeSpan ReplenishmentPeriod { get; set; }

    /// <summary>
    /// How many tokens one period adds, up to <see cref="TokenLimit"/>.
    /// </summary>
    public int TokensPerPeriod { get; set; }

    internal void Validate()
    {
        ValidateCommon();

        if (TokenLimit <= 0)
            throw new ArgumentException("TokenLimit must be greater than zero.", nameof(TokenLimit));

        if (TokensPerPeriod <= 0)
            throw new ArgumentException("TokensPerPeriod must be greater than zero.", nameof(TokensPerPeriod));

        if (ReplenishmentPeriod.TotalMilliseconds < 1)
            throw new ArgumentException("ReplenishmentPeriod must be at least one millisecond.", nameof(ReplenishmentPeriod));
    }
}

/// <summary>
/// Settings of <see cref="KahunaConcurrencyLimiter"/>.
/// </summary>
public sealed class KahunaConcurrencyLimiterOptions : KahunaRateLimiterOptions
{
    /// <summary>
    /// How many permits may be held at once, across every process that shares the key.
    /// </summary>
    public int PermitLimit { get; set; }

    /// <summary>
    /// How long a held permit survives on the cluster without a renewal. The process that holds a
    /// permit renews it every third of this period until the lease is disposed, so a permit only
    /// lapses when its holder dies or loses the cluster. A shorter lease returns the permits of a
    /// dead process sooner, and costs more renewals.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long a queued waiter sleeps before it asks the cluster again. A permit released by
    /// another process is only seen at the next attempt. A permit released by this process wakes
    /// the queue at once.
    /// </summary>
    public TimeSpan QueuePollInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    internal void Validate()
    {
        ValidateCommon();

        if (PermitLimit <= 0)
            throw new ArgumentException("PermitLimit must be greater than zero.", nameof(PermitLimit));

        if (LeaseDuration.TotalMilliseconds < 30)
            throw new ArgumentException("LeaseDuration must be at least 30 milliseconds.", nameof(LeaseDuration));

        if (QueuePollInterval.TotalMilliseconds < 1)
            throw new ArgumentException("QueuePollInterval must be at least one millisecond.", nameof(QueuePollInterval));
    }
}

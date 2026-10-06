/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// A sliding-window limiter whose budget lives in Kahuna: at most
/// <see cref="KahunaSlidingWindowRateLimiterOptions.PermitLimit"/> permits in any window, across
/// every process that names the same key.
///
/// The window is cut into segments. The permits spent in a segment come back when that segment
/// slides out of the window, so a burst at the end of one window cannot be followed by a full burst
/// at the start of the next, which a fixed window allows.
/// </summary>
public sealed class KahunaSlidingWindowRateLimiter : KahunaRateLimiter
{
    private readonly KahunaRateLimiterScript script;

    private readonly string limitText;

    private readonly string segmentText;

    private readonly string segmentsText;

    public KahunaSlidingWindowRateLimiter(KahunaClient client, KahunaSlidingWindowRateLimiterOptions options)
        : base(client, Validated(options), options.PermitLimit)
    {
        script = KahunaRateLimiterScripts.Select(options.Durability, KahunaRateLimiterScripts.EphemeralSlidingWindow, KahunaRateLimiterScripts.PersistentSlidingWindow);
        limitText = options.PermitLimit.ToString();
        segmentText = ((long)options.Window.TotalMilliseconds / options.SegmentsPerWindow).ToString();
        segmentsText = options.SegmentsPerWindow.ToString();
    }

    private protected override async Task<KahunaRateLimitLease> AttemptOnClusterAsync(int permitCount, CancellationToken cancellationToken)
    {
        List<KeyValueParameter> parameters =
        [
            new() { Key = "@key", Value = Key },
            new() { Key = "@segment_ms", Value = segmentText },
            new() { Key = "@segments", Value = segmentsText },
            new() { Key = "@limit", Value = limitText },
            new() { Key = "@permits", Value = PermitsText(permitCount) }
        ];

        (bool granted, long value) = await DecideAsync(script, parameters, cancellationToken).ConfigureAwait(false);

        if (!granted)
            return Refusal(value);

        ObserveAvailablePermits(value);
        return KahunaRateLimitLease.Granted;
    }

    private static KahunaSlidingWindowRateLimiterOptions Validated(KahunaSlidingWindowRateLimiterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return options;
    }
}

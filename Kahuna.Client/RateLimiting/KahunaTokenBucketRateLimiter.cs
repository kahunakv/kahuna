/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// A token-bucket limiter whose bucket lives in Kahuna, shared by every process that names the same
/// key. A burst may spend up to <see cref="KahunaTokenBucketRateLimiterOptions.TokenLimit"/> tokens,
/// and the bucket then refills by <see cref="KahunaTokenBucketRateLimiterOptions.TokensPerPeriod"/>
/// tokens every <see cref="KahunaTokenBucketRateLimiterOptions.ReplenishmentPeriod"/>.
///
/// No process runs a refill timer. Each decision computes the refill from the cluster clock, so the
/// bucket refills on schedule even while no process is running.
/// </summary>
public sealed class KahunaTokenBucketRateLimiter : KahunaRateLimiter
{
    private readonly KahunaRateLimiterScript script;

    private readonly string limitText;

    private readonly string periodText;

    private readonly string perPeriodText;

    public KahunaTokenBucketRateLimiter(KahunaClient client, KahunaTokenBucketRateLimiterOptions options)
        : base(client, Validated(options), options.TokenLimit)
    {
        script = KahunaRateLimiterScripts.Select(options.Durability, KahunaRateLimiterScripts.EphemeralTokenBucket, KahunaRateLimiterScripts.PersistentTokenBucket);
        limitText = options.TokenLimit.ToString();
        periodText = ((long)options.ReplenishmentPeriod.TotalMilliseconds).ToString();
        perPeriodText = options.TokensPerPeriod.ToString();
    }

    private protected override async Task<KahunaRateLimitLease> AttemptOnClusterAsync(int permitCount, CancellationToken cancellationToken)
    {
        List<KeyValueParameter> parameters =
        [
            new() { Key = "@key", Value = Key },
            new() { Key = "@limit", Value = limitText },
            new() { Key = "@period_ms", Value = periodText },
            new() { Key = "@per_period", Value = perPeriodText },
            new() { Key = "@permits", Value = PermitsText(permitCount) }
        ];

        (bool granted, long value) = await RunScriptAsync(script, parameters, cancellationToken).ConfigureAwait(false);

        if (!granted)
            return Refusal(value);

        ObserveAvailablePermits(value);
        return KahunaRateLimitLease.Granted;
    }

    private static KahunaTokenBucketRateLimiterOptions Validated(KahunaTokenBucketRateLimiterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return options;
    }
}

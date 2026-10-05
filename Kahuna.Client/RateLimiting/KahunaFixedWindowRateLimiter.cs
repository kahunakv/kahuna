/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// A fixed-window limiter whose budget lives in Kahuna: at most
/// <see cref="KahunaFixedWindowRateLimiterOptions.PermitLimit"/> permits per window, across every
/// process that names the same key. Windows are aligned to the cluster clock, and the budget resets
/// on each window boundary whatever happened in the window before.
/// </summary>
public sealed class KahunaFixedWindowRateLimiter : KahunaRateLimiter
{
    private readonly KahunaRateLimiterScript script;

    private readonly string limitText;

    private readonly string windowText;

    public KahunaFixedWindowRateLimiter(KahunaClient client, KahunaFixedWindowRateLimiterOptions options)
        : base(client, Validated(options), options.PermitLimit)
    {
        script = KahunaRateLimiterScripts.Select(options.Durability, KahunaRateLimiterScripts.EphemeralFixedWindow, KahunaRateLimiterScripts.PersistentFixedWindow);
        limitText = options.PermitLimit.ToString();
        windowText = ((long)options.Window.TotalMilliseconds).ToString();
    }

    private protected override async Task<KahunaRateLimitLease> AttemptOnClusterAsync(int permitCount, CancellationToken cancellationToken)
    {
        List<KeyValueParameter> parameters =
        [
            new() { Key = "@key", Value = Key },
            new() { Key = "@window_ms", Value = windowText },
            new() { Key = "@limit", Value = limitText },
            new() { Key = "@permits", Value = PermitsText(permitCount) }
        ];

        (bool granted, long value) = await RunScriptAsync(script, parameters, cancellationToken).ConfigureAwait(false);

        if (!granted)
            return Refusal(value);

        ObserveAvailablePermits(value);
        return KahunaRateLimitLease.Granted;
    }

    private static KahunaFixedWindowRateLimiterOptions Validated(KahunaFixedWindowRateLimiterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return options;
    }
}

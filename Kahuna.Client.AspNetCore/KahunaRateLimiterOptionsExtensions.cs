/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.RateLimiting;
using Kahuna.Client;
using Kahuna.Client.RateLimiting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.AspNetCore.RateLimiting;

/// <summary>
/// Adds rate-limiting policies whose budgets live in Kahuna, so every replica of the application
/// spends one budget instead of one budget each.
///
/// <para>Each method has two forms. The form without a partition gives the policy one budget for
/// every request. The form with <c>partitionBy</c> gives each partition, for example each user or
/// each client address, a budget of its own.</para>
///
/// <para>The Kahuna key of a policy is <c>rate-limit/&lt;policy&gt;</c>, and that of a partition is
/// <c>rate-limit/&lt;policy&gt;/&lt;partition&gt;</c>. The <c>configure</c> callback sees that key in
/// <see cref="KahunaRateLimiterOptions.Key"/> and may replace it. It runs once when the policy is
/// added, to check the settings at startup, and once for each partition the policy creates.</para>
///
/// <para>The <see cref="KahunaClient"/> is the <c>client</c> argument when one is passed, and the
/// one registered in the request services otherwise.</para>
///
/// <code>
/// builder.Services.AddSingleton(new KahunaClient(["https://kahuna1:8082", "https://kahuna2:8084"]));
///
/// builder.Services.AddRateLimiter(options =>
/// {
///     options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
///
///     options.AddKahunaFixedWindowLimiter("simple_endpoints", o =>
///     {
///         o.PermitLimit = 100;
///         o.Window = TimeSpan.FromMinutes(1);
///     });
///
///     options.AddKahunaTokenBucketLimiter("login_attempts",
///         context => context.Request.Query["email"].ToString(),
///         o =>
///         {
///             o.TokenLimit = 5;
///             o.ReplenishmentPeriod = TimeSpan.FromMinutes(5);
///             o.TokensPerPeriod = 1;
///         });
/// });
/// </code>
/// </summary>
public static class KahunaRateLimiterOptionsExtensions
{
    /// <summary>
    /// Adds a fixed-window policy: at most <see cref="KahunaFixedWindowRateLimiterOptions.PermitLimit"/>
    /// requests per window, across every replica.
    /// </summary>
    public static RateLimiterOptions AddKahunaFixedWindowLimiter(
        this RateLimiterOptions options,
        string policyName,
        Action<KahunaFixedWindowRateLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, null, configure, client, static o => o.Validate(), static (c, o) => new KahunaFixedWindowRateLimiter(c, o));

    /// <summary>
    /// Adds a fixed-window policy with a separate budget for each partition.
    /// </summary>
    public static RateLimiterOptions AddKahunaFixedWindowLimiter(
        this RateLimiterOptions options,
        string policyName,
        Func<HttpContext, string> partitionBy,
        Action<KahunaFixedWindowRateLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, Required(partitionBy), configure, client, static o => o.Validate(), static (c, o) => new KahunaFixedWindowRateLimiter(c, o));

    /// <summary>
    /// Adds a sliding-window policy: at most <see cref="KahunaSlidingWindowRateLimiterOptions.PermitLimit"/>
    /// requests in any window, across every replica.
    /// </summary>
    public static RateLimiterOptions AddKahunaSlidingWindowLimiter(
        this RateLimiterOptions options,
        string policyName,
        Action<KahunaSlidingWindowRateLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, null, configure, client, static o => o.Validate(), static (c, o) => new KahunaSlidingWindowRateLimiter(c, o));

    /// <summary>
    /// Adds a sliding-window policy with a separate budget for each partition.
    /// </summary>
    public static RateLimiterOptions AddKahunaSlidingWindowLimiter(
        this RateLimiterOptions options,
        string policyName,
        Func<HttpContext, string> partitionBy,
        Action<KahunaSlidingWindowRateLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, Required(partitionBy), configure, client, static o => o.Validate(), static (c, o) => new KahunaSlidingWindowRateLimiter(c, o));

    /// <summary>
    /// Adds a token-bucket policy: bursts up to <see cref="KahunaTokenBucketRateLimiterOptions.TokenLimit"/>
    /// requests, refilled over time, across every replica.
    /// </summary>
    public static RateLimiterOptions AddKahunaTokenBucketLimiter(
        this RateLimiterOptions options,
        string policyName,
        Action<KahunaTokenBucketRateLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, null, configure, client, static o => o.Validate(), static (c, o) => new KahunaTokenBucketRateLimiter(c, o));

    /// <summary>
    /// Adds a token-bucket policy with a separate bucket for each partition.
    /// </summary>
    public static RateLimiterOptions AddKahunaTokenBucketLimiter(
        this RateLimiterOptions options,
        string policyName,
        Func<HttpContext, string> partitionBy,
        Action<KahunaTokenBucketRateLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, Required(partitionBy), configure, client, static o => o.Validate(), static (c, o) => new KahunaTokenBucketRateLimiter(c, o));

    /// <summary>
    /// Adds a concurrency policy: at most <see cref="KahunaConcurrencyLimiterOptions.PermitLimit"/>
    /// requests in progress at once, across every replica. A permit comes back when its request ends.
    /// </summary>
    public static RateLimiterOptions AddKahunaConcurrencyLimiter(
        this RateLimiterOptions options,
        string policyName,
        Action<KahunaConcurrencyLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, null, configure, client, static o => o.Validate(), static (c, o) => new KahunaConcurrencyLimiter(c, o));

    /// <summary>
    /// Adds a concurrency policy with a separate set of permits for each partition.
    /// </summary>
    public static RateLimiterOptions AddKahunaConcurrencyLimiter(
        this RateLimiterOptions options,
        string policyName,
        Func<HttpContext, string> partitionBy,
        Action<KahunaConcurrencyLimiterOptions> configure,
        KahunaClient? client = null
    ) => AddKahunaPolicy(options, policyName, Required(partitionBy), configure, client, static o => o.Validate(), static (c, o) => new KahunaConcurrencyLimiter(c, o));

    private static RateLimiterOptions AddKahunaPolicy<TOptions>(
        RateLimiterOptions options,
        string policyName,
        Func<HttpContext, string>? partitionBy,
        Action<TOptions> configure,
        KahunaClient? client,
        Action<TOptions> validate,
        Func<KahunaClient, TOptions, RateLimiter> create
    ) where TOptions : KahunaRateLimiterOptions, new()
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(policyName);
        ArgumentNullException.ThrowIfNull(configure);

        // A setting that is wrong fails here, at startup, rather than on the first request.
        validate(Configure(policyName, partitionBy is null ? null : "partition", configure));

        return options.AddPolicy(policyName, context =>
        {
            string partition = partitionBy?.Invoke(context) ?? "";

            KahunaClient kahuna = client ?? context.RequestServices.GetRequiredService<KahunaClient>();

            return RateLimitPartition.Get(partition, key => create(kahuna, Configure(policyName, partitionBy is null ? null : key, configure)));
        });
    }

    private static TOptions Configure<TOptions>(string policyName, string? partition, Action<TOptions> configure)
        where TOptions : KahunaRateLimiterOptions, new()
    {
        TOptions options = new() { Key = KahunaRateLimitPartition.KeyFor(policyName, partition) };
        configure(options);
        return options;
    }

    private static Func<HttpContext, string> Required(Func<HttpContext, string> partitionBy)
    {
        ArgumentNullException.ThrowIfNull(partitionBy);
        return partitionBy;
    }
}

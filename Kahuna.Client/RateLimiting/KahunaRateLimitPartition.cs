/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.RateLimiting;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// Builds <see cref="RateLimitPartition{TKey}"/> values backed by Kahuna, for
/// <see cref="PartitionedRateLimiter.Create{TResource, TPartitionKey}"/> and for the
/// <c>AddPolicy</c> overloads of the ASP.NET Core rate-limiting options.
///
/// Each partition gets its own limiter, over the key that the options factory names for it. Give
/// every partition a different <see cref="KahunaRateLimiterOptions.Key"/>, for example by appending
/// the partition key to a fixed prefix with <see cref="KeyFor"/>. Partitions that name the same key
/// share one budget.
///
/// <code>
/// options.AddPolicy("login_attempts", context =>
/// {
///     string email = context.Request.Query["email"].ToString();
///
///     return KahunaRateLimitPartition.GetTokenBucketLimiter(kahuna, email, key => new()
///     {
///         Key = KahunaRateLimitPartition.KeyFor("login_attempts", key),
///         TokenLimit = 5,
///         ReplenishmentPeriod = TimeSpan.FromMinutes(5),
///         TokensPerPeriod = 1
///     });
/// });
/// </code>
/// </summary>
public static class KahunaRateLimitPartition
{
    /// <summary>
    /// The prefix that <see cref="KeyFor"/> puts in front of every key it names.
    /// </summary>
    public const string DefaultKeyPrefix = "rate-limit/";

    /// <summary>
    /// Names the Kahuna key of one partition of a policy:
    /// <c>rate-limit/&lt;policy&gt;</c>, or <c>rate-limit/&lt;policy&gt;/&lt;partition&gt;</c> when there is
    /// a partition. The partition key is part of the stored key, so keep it free of personal data you
    /// do not want in the cluster, or hash it first.
    /// </summary>
    public static string KeyFor(string policyName, string? partitionKey = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(policyName);

        return string.IsNullOrEmpty(partitionKey)
            ? string.Concat(DefaultKeyPrefix, policyName)
            : string.Concat(DefaultKeyPrefix, policyName, "/", partitionKey);
    }

    public static RateLimitPartition<TKey> GetFixedWindowLimiter<TKey>(
        KahunaClient client, TKey partitionKey, Func<TKey, KahunaFixedWindowRateLimiterOptions> factory)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(factory);

        return RateLimitPartition.Get(partitionKey, key => new KahunaFixedWindowRateLimiter(client, factory(key)));
    }

    public static RateLimitPartition<TKey> GetSlidingWindowLimiter<TKey>(
        KahunaClient client, TKey partitionKey, Func<TKey, KahunaSlidingWindowRateLimiterOptions> factory)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(factory);

        return RateLimitPartition.Get(partitionKey, key => new KahunaSlidingWindowRateLimiter(client, factory(key)));
    }

    public static RateLimitPartition<TKey> GetTokenBucketLimiter<TKey>(
        KahunaClient client, TKey partitionKey, Func<TKey, KahunaTokenBucketRateLimiterOptions> factory)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(factory);

        return RateLimitPartition.Get(partitionKey, key => new KahunaTokenBucketRateLimiter(client, factory(key)));
    }

    public static RateLimitPartition<TKey> GetConcurrencyLimiter<TKey>(
        KahunaClient client, TKey partitionKey, Func<TKey, KahunaConcurrencyLimiterOptions> factory)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(factory);

        return RateLimitPartition.Get(partitionKey, key => new KahunaConcurrencyLimiter(client, factory(key)));
    }
}

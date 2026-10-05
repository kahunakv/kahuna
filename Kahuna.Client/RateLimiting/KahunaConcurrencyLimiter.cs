/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// A concurrency limiter whose permits live in Kahuna: at most
/// <see cref="KahunaConcurrencyLimiterOptions.PermitLimit"/> permits are held at once, across every
/// process that names the same key. A permit comes back when its lease is disposed.
///
/// <para>Every held lease is a key directly under <c>&lt;Key&gt;/</c>, so this limiter owns that
/// key space: do not store anything else directly under it. The lease key expires unless its holder
/// renews it, which this process does every third of
/// <see cref="KahunaConcurrencyLimiterOptions.LeaseDuration"/> until the lease is disposed. The
/// permits of a process that dies come back when its leases expire.</para>
///
/// <para>A disposed lease gives its permits back in the background, because
/// <see cref="IDisposable.Dispose"/> must not wait for the cluster. If that fails after its retries,
/// the lease key is left to expire, and the permits come back at the expiry instead. A permit whose
/// renewal fails for a whole lease period lapses while its holder still runs, and another process
/// can then take it.</para>
/// </summary>
public sealed class KahunaConcurrencyLimiter : KahunaRateLimiter
{
    private readonly KahunaRateLimiterScript acquireScript;

    private readonly KahunaRateLimiterScript renewScript;

    private readonly KahunaRateLimiterScript releaseScript;

    private readonly string limitText;

    private readonly string leaseText;

    private readonly TimeSpan renewInterval;

    private readonly TimeSpan pollInterval;

    /// <summary>The leases this process holds, by lease key, with the permits each one took.</summary>
    private readonly ConcurrentDictionary<string, int> heldLeases = new(StringComparer.Ordinal);

    /// <summary>Guards the start and the end of the renewal loop against a lease taken meanwhile.</summary>
    private readonly Lock renewalLock = new();

    private bool renewalRunning;

    /// <summary>Releases still in flight, so an asynchronous disposal can wait for them.</summary>
    private readonly ConcurrentDictionary<Task, byte> pendingReleases = new();

    public KahunaConcurrencyLimiter(KahunaClient client, KahunaConcurrencyLimiterOptions options)
        : base(client, Validated(options), options.PermitLimit)
    {
        acquireScript = KahunaRateLimiterScripts.Select(options.Durability, KahunaRateLimiterScripts.EphemeralConcurrencyAcquire, KahunaRateLimiterScripts.PersistentConcurrencyAcquire);
        renewScript = KahunaRateLimiterScripts.Select(options.Durability, KahunaRateLimiterScripts.EphemeralConcurrencyRenew, KahunaRateLimiterScripts.PersistentConcurrencyRenew);
        releaseScript = KahunaRateLimiterScripts.Select(options.Durability, KahunaRateLimiterScripts.EphemeralConcurrencyRelease, KahunaRateLimiterScripts.PersistentConcurrencyRelease);
        limitText = options.PermitLimit.ToString();
        leaseText = ((long)options.LeaseDuration.TotalMilliseconds).ToString();
        renewInterval = options.LeaseDuration / 3;
        pollInterval = options.QueuePollInterval;
    }

    private protected override bool HasHeldPermits => !heldLeases.IsEmpty;

    private protected override TimeSpan QueuePollInterval => pollInterval;

    private protected override async Task<KahunaRateLimitLease> AttemptOnClusterAsync(int permitCount, CancellationToken cancellationToken)
    {
        string leaseKey = string.Concat(Key, "/", Guid.NewGuid().ToString("N"));
        string permitsText = PermitsText(permitCount);

        List<KeyValueParameter> parameters =
        [
            new() { Key = "@bucket", Value = Key },
            new() { Key = "@lease_key", Value = leaseKey },
            new() { Key = "@limit", Value = limitText },
            new() { Key = "@permits", Value = permitsText },
            new() { Key = "@lease_ms", Value = leaseText }
        ];

        (bool granted, long value) = await RunScriptAsync(acquireScript, parameters, cancellationToken).ConfigureAwait(false);

        if (!granted)
            return Refusal(value);

        ObserveAvailablePermits(value);

        // Zero permits asks whether permits are free, and writes no lease.
        if (permitCount == 0)
            return KahunaRateLimitLease.Granted;

        Hold(leaseKey, permitCount);

        return new(true, null, () => Release(leaseKey));
    }

    private void Hold(string leaseKey, int permitCount)
    {
        heldLeases[leaseKey] = permitCount;

        lock (renewalLock)
        {
            if (renewalRunning)
                return;

            renewalRunning = true;
        }

        _ = RenewLeasesAsync();
    }

    private void Release(string leaseKey)
    {
        if (!heldLeases.TryRemove(leaseKey, out _))
            return;

        Task release = ReleaseOnClusterAsync(leaseKey);

        pendingReleases.TryAdd(release, 0);

        _ = release.ContinueWith(
            static (task, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(task, out _),
            pendingReleases,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        );
    }

    /// <summary>
    /// Deletes the lease key, and then wakes the queue, because a waiter of this process can take
    /// the permits now. Runs without a cancellation token: disposal of the limiter does not stop
    /// permits from coming back.
    /// </summary>
    private async Task ReleaseOnClusterAsync(string leaseKey)
    {
        try
        {
            List<KeyValueParameter> parameters = [new() { Key = "@lease_key", Value = leaseKey }];

            await RunScriptAsync(releaseScript, parameters, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The lease key expires on its own, and the permits come back then. Nothing else can
            // be done for a release that failed after its retries.
        }
        finally
        {
            WakeQueue();
        }
    }

    /// <summary>
    /// Renews every held lease once per renewal interval, and stops when no lease is held. A lease
    /// that expired before its renewal is not written again, because its permits may already
    /// belong to another process.
    /// </summary>
    private async Task RenewLeasesAsync()
    {
        CancellationToken disposeToken = DisposeToken;

        try
        {
            while (true)
            {
                await Task.Delay(renewInterval, disposeToken).ConfigureAwait(false);

                foreach (KeyValuePair<string, int> lease in heldLeases)
                {
                    List<KeyValueParameter> parameters =
                    [
                        new() { Key = "@lease_key", Value = lease.Key },
                        new() { Key = "@permits", Value = PermitsText(lease.Value) },
                        new() { Key = "@lease_ms", Value = leaseText }
                    ];

                    try
                    {
                        await RunScriptAsync(renewScript, parameters, disposeToken).ConfigureAwait(false);
                    }
                    catch (Exception) when (!disposeToken.IsCancellationRequested)
                    {
                        // The next round tries again. The lease only lapses if every renewal fails
                        // for a whole lease period.
                    }
                }

                lock (renewalLock)
                {
                    if (heldLeases.IsEmpty)
                    {
                        renewalRunning = false;
                        return;
                    }
                }
            }
        }
        catch (Exception) when (disposeToken.IsCancellationRequested)
        {
            // Disposal stops the renewals. Leases still held then expire on the cluster unless
            // their holders dispose them first.
            lock (renewalLock)
                renewalRunning = false;
        }
    }

    private protected override async Task WaitForBackgroundWorkAsync()
    {
        foreach (Task release in pendingReleases.Keys)
            await release.ConfigureAwait(false);
    }

    private static KahunaConcurrencyLimiterOptions Validated(KahunaConcurrencyLimiterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return options;
    }
}

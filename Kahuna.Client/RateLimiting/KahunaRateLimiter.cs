/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Text;
using System.Diagnostics;
using System.Text;
using System.Threading.RateLimiting;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client.RateLimiting;

/// <summary>
/// Base of the limiters whose budget lives in Kahuna, so every process that names the same key
/// spends one budget between them.
///
/// <para>A decision needs a round trip to the cluster, so it is only made on the asynchronous path.
/// <see cref="RateLimiter.AttemptAcquire(int)"/> never blocks on I/O: it refuses at once, and a
/// caller that can wait follows it with <see cref="RateLimiter.AcquireAsync(int, CancellationToken)"/>.
/// The ASP.NET Core rate-limiting middleware does exactly that.</para>
///
/// <para>A request that the budget refuses can wait in a queue that is local to this process. One
/// background loop serves the queue: it asks the cluster again for the waiter at the head, sleeping
/// in between for as long as the cluster said the budget stays spent.</para>
/// </summary>
public abstract class KahunaRateLimiter : RateLimiter
{
    private readonly KahunaClient client;

    // The settings are copied at construction, so changing the options object later changes nothing.
    private readonly KeyValueDurability durability;

    private readonly int queueLimit;

    private readonly QueueProcessingOrder queueProcessingOrder;

    private readonly KahunaRateLimiterFailureMode failureMode;

    private readonly int maxRetries;

    /// <summary>Guards the queue, the queued-permit count and the loop that serves the queue.</summary>
    private readonly Lock queueLock = new();

    private readonly LinkedList<Waiter> queue = new();

    /// <summary>
    /// Cancelled by disposal. Never disposed itself: the background loops read its token after
    /// disposal starts, and a source with no timer holds nothing that needs freeing.
    /// </summary>
    private readonly CancellationTokenSource disposeSource = new();

    private int queuedPermits;

    private bool queueLoopRunning;

    private int disposed;

    /// <summary>Requests inside <see cref="AcquireOnClusterAsync"/>, queued ones included.</summary>
    private int activeRequests;

    private long idleSinceTimestamp = Stopwatch.GetTimestamp();

    private long successfulLeases;

    private long failedLeases;

    private long lastKnownAvailablePermits;

    /// <summary>Completed to wake the queue loop before its sleep ends, and replaced on every wake.</summary>
    private TaskCompletionSource wakeSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private protected KahunaRateLimiter(KahunaClient client, KahunaRateLimiterOptions options, int permitLimit)
    {
        ArgumentNullException.ThrowIfNull(client);

        this.client = client;
        Key = options.Key;
        durability = options.Durability;
        queueLimit = options.QueueLimit;
        queueProcessingOrder = options.QueueProcessingOrder;
        failureMode = options.FailureMode;
        maxRetries = options.MaxRetries;
        PermitLimit = permitLimit;
        lastKnownAvailablePermits = permitLimit;
    }

    /// <summary>The Kahuna key that holds this limiter's state.</summary>
    public string Key { get; }

    /// <summary>The largest permit count one request may ask for.</summary>
    protected int PermitLimit { get; }

    private protected KeyValueDurability Durability => durability;

    private protected bool IsDisposed => Volatile.Read(ref disposed) == 1;

    private protected CancellationToken DisposeToken => disposeSource.Token;

    /// <summary>
    /// The time this limiter has done nothing for, or null while it has work in progress. A
    /// partitioned limiter disposes a partition that stays idle, and a later request for that
    /// partition builds a new limiter over the same key, so no budget is lost.
    /// </summary>
    public override TimeSpan? IdleDuration
    {
        get
        {
            if (Volatile.Read(ref activeRequests) > 0 || HasHeldPermits)
                return null;

            return Stopwatch.GetElapsedTime(Volatile.Read(ref idleSinceTimestamp));
        }
    }

    /// <summary>True while this process holds permits that it must give back.</summary>
    private protected virtual bool HasHeldPermits => false;

    /// <summary>
    /// Counts that this process observed. The available permits are the ones the cluster reported
    /// on the last decision, so other processes may have spent them since.
    /// </summary>
    public override RateLimiterStatistics GetStatistics()
    {
        ThrowIfDisposed();

        return new()
        {
            CurrentAvailablePermits = Volatile.Read(ref lastKnownAvailablePermits),
            CurrentQueuedCount = Volatile.Read(ref queuedPermits),
            TotalFailedLeases = Interlocked.Read(ref failedLeases),
            TotalSuccessfulLeases = Interlocked.Read(ref successfulLeases)
        };
    }

    /// <summary>
    /// Always refuses, because a decision needs a round trip to the cluster and this path must not
    /// block. Use <see cref="RateLimiter.AcquireAsync(int, CancellationToken)"/> to get a decision.
    /// </summary>
    protected override RateLimitLease AttemptAcquireCore(int permitCount)
    {
        ValidatePermitCount(permitCount);
        ThrowIfDisposed();

        return KahunaRateLimitLease.Refused;
    }

    /// <summary>
    /// Checks the request before the first await, so an oversized request or a disposed limiter
    /// throws at the call, as it does for the framework's own limiters.
    /// </summary>
    protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken)
    {
        ValidatePermitCount(permitCount);
        ThrowIfDisposed();

        return AcquireOnClusterAsync(permitCount, cancellationToken);
    }

    private async ValueTask<RateLimitLease> AcquireOnClusterAsync(int permitCount, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref activeRequests);

        try
        {
            // A new request does not overtake older waiters, unless the queue serves the newest
            // first. That is the order the in-process limiters of the framework keep.
            bool attemptNow;

            lock (queueLock)
                attemptNow = queue.Count == 0 || queueProcessingOrder == QueueProcessingOrder.NewestFirst;

            KahunaRateLimitLease? refusal = null;

            if (attemptNow)
            {
                KahunaRateLimitLease lease = await AttemptAsync(permitCount, cancellationToken).ConfigureAwait(false);

                if (lease.IsAcquired)
                {
                    Interlocked.Increment(ref successfulLeases);
                    return lease;
                }

                refusal = lease;
            }

            if (queueLimit == 0 || permitCount > queueLimit)
            {
                Interlocked.Increment(ref failedLeases);
                return refusal ?? KahunaRateLimitLease.Refused;
            }

            return await WaitInQueueAsync(permitCount, refusal, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (Interlocked.Decrement(ref activeRequests) == 0)
                Volatile.Write(ref idleSinceTimestamp, Stopwatch.GetTimestamp());
        }
    }

    /// <summary>
    /// Asks the cluster for permits once. Returns a granted lease or a refused one; throws only for
    /// cancellation, or for a failure when <see cref="KahunaRateLimiterOptions.FailureMode"/> says so.
    /// </summary>
    private protected abstract Task<KahunaRateLimitLease> AttemptOnClusterAsync(int permitCount, CancellationToken cancellationToken);

    private async Task<KahunaRateLimitLease> AttemptAsync(int permitCount, CancellationToken cancellationToken)
    {
        try
        {
            return await AttemptOnClusterAsync(permitCount, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            // The transport reports a cancelled call as an aborted operation. The caller asked for
            // the cancellation, so it gets the cancellation.
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception) when (failureMode == KahunaRateLimiterFailureMode.Allow)
        {
            return KahunaRateLimitLease.Granted;
        }
        catch (Exception) when (failureMode == KahunaRateLimiterFailureMode.Deny)
        {
            return KahunaRateLimitLease.Refused;
        }
    }

    /// <summary>
    /// Runs one decision script, and runs it again while the cluster answers that the transaction
    /// aborted or must be retried. Neither outcome changed the state, so a retry cannot spend
    /// permits twice. Two callers that race for one counter are what makes a transaction abort.
    /// </summary>
    private protected async Task<(bool Granted, long Value)> RunScriptAsync(
        KahunaRateLimiterScript script,
        List<KeyValueParameter> parameters,
        CancellationToken cancellationToken
    )
    {
        for (int attempt = 0; ; attempt++)
        {
            KahunaKeyValueTransactionResult result;

            try
            {
                result = await client.ExecuteKeyValueTransactionScript(script.Bytes, script.Hash, parameters, cancellationToken).ConfigureAwait(false);
            }
            catch (KahunaException ex) when (
                ex.ErrorDomain == KahunaErrorDomain.KeyValue
                && ex.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry
                && attempt < maxRetries
                && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(RetryDelay(attempt), cancellationToken).ConfigureAwait(false);
                continue;
            }

            return ParseAnswer(result);
        }
    }

    /// <summary>
    /// A random delay that grows with the attempt, so callers that collided once do not collide
    /// again in lockstep: up to 2, 4, 8, … ms, and never more than 64 ms.
    /// </summary>
    private static TimeSpan RetryDelay(int attempt) =>
        TimeSpan.FromMilliseconds(Random.Shared.Next(1, 2 << Math.Min(attempt, 5)));

    /// <summary>
    /// Reads <c>"1:&lt;n&gt;"</c> or <c>"0:&lt;n&gt;"</c>, the only answers the scripts give.
    /// </summary>
    private static (bool Granted, long Value) ParseAnswer(KahunaKeyValueTransactionResult result)
    {
        ReadOnlySpan<byte> answer = result.FirstValue;

        if (answer.Length >= 3
            && answer[0] is (byte)'0' or (byte)'1'
            && answer[1] == (byte)':'
            && Utf8Parser.TryParse(answer[2..], out long value, out int consumed)
            && consumed == answer.Length - 2)
            return (answer[0] == (byte)'1', value);

        throw new KahunaException(
            $"Rate limiter script returned an unexpected answer '{Encoding.UTF8.GetString(answer)}' ({result.Type})",
            KeyValueResponseType.Errored
        );
    }

    /// <summary>Records the permits the cluster reported as left, for the statistics.</summary>
    private protected void ObserveAvailablePermits(long available) =>
        Volatile.Write(ref lastKnownAvailablePermits, Math.Max(0, available));

    /// <summary>
    /// A refusal that carries the time until the budget can cover the request, when the cluster
    /// could tell. Statistics treat a refusal as zero permits left.
    /// </summary>
    private protected KahunaRateLimitLease Refusal(long retryAfterMs)
    {
        ObserveAvailablePermits(0);

        return retryAfterMs > 0
            ? new(false, TimeSpan.FromMilliseconds(retryAfterMs), null)
            : KahunaRateLimitLease.Refused;
    }

    private protected static string PermitsText(int permitCount) => permitCount switch
    {
        0 => "0",
        1 => "1",
        _ => permitCount.ToString()
    };

    /// <summary>Wakes the queue loop before its sleep ends, because permits came back.</summary>
    private protected void WakeQueue()
    {
        TaskCompletionSource previous = Interlocked.Exchange(ref wakeSignal, new(TaskCreationOptions.RunContinuationsAsynchronously));
        previous.TrySetResult();
    }

    private async Task<RateLimitLease> WaitInQueueAsync(int permitCount, KahunaRateLimitLease? refusal, CancellationToken cancellationToken)
    {
        Waiter waiter = new(permitCount);
        List<Waiter>? evicted = null;
        bool startLoop = false;

        lock (queueLock)
        {
            ThrowIfDisposed();

            if (queuedPermits + permitCount > queueLimit)
            {
                if (queueProcessingOrder == QueueProcessingOrder.OldestFirst)
                {
                    Interlocked.Increment(ref failedLeases);
                    return refusal ?? KahunaRateLimitLease.Refused;
                }

                // The newest is served first, so the oldest waiters make room for it.
                while (queuedPermits + permitCount > queueLimit && queue.First is { } oldest)
                {
                    RemoveFromQueue(oldest.Value);
                    (evicted ??= []).Add(oldest.Value);
                }
            }

            waiter.Node = queue.AddLast(waiter);
            queuedPermits += permitCount;

            if (!queueLoopRunning)
            {
                queueLoopRunning = true;
                startLoop = true;
            }
        }

        if (evicted is not null)
        {
            foreach (Waiter oldest in evicted)
            {
                if (oldest.TrySetResult(KahunaRateLimitLease.Refused))
                    Interlocked.Increment(ref failedLeases);
            }
        }

        if (startLoop)
            _ = ServeQueueAsync();

        using CancellationTokenRegistration registration = cancellationToken.Register(
            static state =>
            {
                (KahunaRateLimiter limiter, Waiter waiter, CancellationToken token) = ((KahunaRateLimiter, Waiter, CancellationToken))state!;

                lock (limiter.queueLock)
                    limiter.RemoveFromQueue(waiter);

                waiter.TrySetCanceled(token);
            },
            (this, waiter, cancellationToken)
        );

        return await waiter.Task.ConfigureAwait(false);
    }

    /// <summary>Takes a waiter out of the queue. Must be called under <see cref="queueLock"/>.</summary>
    private bool RemoveFromQueue(Waiter waiter)
    {
        if (waiter.Node is null)
            return false;

        queue.Remove(waiter.Node);
        waiter.Node = null;
        queuedPermits -= waiter.PermitCount;
        return true;
    }

    /// <summary>
    /// Serves the queue until it is empty: asks the cluster for the waiter that is next in order,
    /// hands it the lease when granted, and otherwise sleeps until the cluster said the budget
    /// can cover it, or until this process gives permits back.
    ///
    /// The waiter stays in the queue while its attempt runs, so it keeps counting against the queue
    /// limit. A waiter that was cancelled or evicted during the attempt can no longer take the
    /// lease, and a granted lease it cannot take is disposed at once to give the permits back.
    /// </summary>
    private async Task ServeQueueAsync()
    {
        CancellationToken disposeToken = disposeSource.Token;

        while (true)
        {
            Waiter waiter;
            Task wake;

            lock (queueLock)
            {
                LinkedListNode<Waiter>? next = queueProcessingOrder == QueueProcessingOrder.OldestFirst ? queue.First : queue.Last;

                if (next is null || IsDisposed)
                {
                    queueLoopRunning = false;
                    return;
                }

                waiter = next.Value;

                // Read before the attempt, so permits given back while the attempt runs still wake
                // the sleep that follows it.
                wake = Volatile.Read(ref wakeSignal).Task;
            }

            KahunaRateLimitLease lease;

            try
            {
                lease = await AttemptAsync(waiter.PermitCount, disposeToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (disposeToken.IsCancellationRequested)
            {
                // Disposal already completed every waiter.
                lock (queueLock)
                    queueLoopRunning = false;

                return;
            }
            catch (Exception ex)
            {
                // The failure mode is to throw, so the waiter the attempt was for gets the failure.
                lock (queueLock)
                    RemoveFromQueue(waiter);

                waiter.TrySetException(ex);
                continue;
            }

            if (lease.IsAcquired)
            {
                lock (queueLock)
                    RemoveFromQueue(waiter);

                if (waiter.TrySetResult(lease))
                    Interlocked.Increment(ref successfulLeases);
                else
                    lease.Dispose();

                continue;
            }

            TimeSpan sleep = lease.TryGetMetadata(MetadataName.RetryAfter, out TimeSpan retryAfter) ? retryAfter : QueuePollInterval;

            using CancellationTokenSource sleepSource = CancellationTokenSource.CreateLinkedTokenSource(disposeToken);

            Task delay = Task.Delay(sleep, sleepSource.Token);

            await Task.WhenAny(delay, wake).ConfigureAwait(false);

            // Ends the timer when the wake signal won, instead of leaving it to run out.
            await sleepSource.CancelAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// How long the queue sleeps after a refusal that could not say when the budget frees up.
    /// </summary>
    private protected virtual TimeSpan QueuePollInterval => TimeSpan.FromMilliseconds(50);

    private void ValidatePermitCount(int permitCount)
    {
        if (permitCount < 0 || permitCount > PermitLimit)
            throw new ArgumentOutOfRangeException(nameof(permitCount), permitCount, $"{permitCount} permits exceeds the permit limit of {PermitLimit}.");
    }

    private protected void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(IsDisposed, this);

    /// <summary>
    /// Stops the queue and refuses every waiter. A lease already handed out stays valid, and a
    /// concurrency lease still gives its permits back when it is disposed.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        disposeSource.Cancel();

        List<Waiter> drained;

        lock (queueLock)
        {
            drained = new(queue.Count);

            while (queue.First is { } next)
            {
                drained.Add(next.Value);
                RemoveFromQueue(next.Value);
            }
        }

        foreach (Waiter waiter in drained)
        {
            if (waiter.TrySetResult(KahunaRateLimitLease.Refused))
                Interlocked.Increment(ref failedLeases);
        }

        WakeQueue();
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        Dispose(true);

        await WaitForBackgroundWorkAsync().ConfigureAwait(false);
    }

    /// <summary>Waits for work a subclass runs after a lease is gone, such as giving permits back.</summary>
    private protected virtual Task WaitForBackgroundWorkAsync() => Task.CompletedTask;

    private sealed class Waiter : TaskCompletionSource<RateLimitLease>
    {
        public Waiter(int permitCount) : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            PermitCount = permitCount;
        }

        public int PermitCount { get; }

        /// <summary>The waiter's place in the queue, or null once it left. Guarded by the queue lock.</summary>
        public LinkedListNode<Waiter>? Node { get; set; }
    }
}

using System.Reflection;
using System.Threading.RateLimiting;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Client.RateLimiting;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Drives the Kahuna-backed <see cref="RateLimiter"/> implementations through a real
/// <see cref="KahunaClient"/> against a three-node cluster.
///
/// Each node gets its own client, which stands for one replica of an application. The property every
/// limiter must keep is that the replicas spend one budget between them, and that it holds when they
/// race: a replica that kept its own count would admit the budget once per replica.
/// </summary>
public class TestKahunaRateLimiters : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ILoggerFactory loggerFactory;

    public TestKahunaRateLimiters(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static string NewKey() => "rate-limit/test/" + Guid.NewGuid().ToString("N")[..8];

    private static KahunaClient[] Replicas(params IKahuna[] nodes) =>
        nodes.Select(node => new KahunaClient("http://localhost", communication: new InProcessKahunaCommunication(node))).ToArray();

    private static async Task<(int Granted, int Refused, List<RateLimitLease> Leases)> AcquireConcurrently(RateLimiter[] limiters, int requests)
    {
        Task<RateLimitLease>[] attempts = new Task<RateLimitLease>[requests];

        for (int i = 0; i < requests; i++)
            attempts[i] = limiters[i % limiters.Length].AcquireAsync(1, Token).AsTask();

        RateLimitLease[] leases = await Task.WhenAll(attempts);

        int granted = leases.Count(lease => lease.IsAcquired);

        return (granted, requests - granted, leases.ToList());
    }

    private static TimeSpan RetryAfter(RateLimitLease lease)
    {
        Assert.False(lease.IsAcquired);
        Assert.True(lease.TryGetMetadata(MetadataName.RetryAfter, out TimeSpan retryAfter), "A refusal should say when to retry");
        return retryAfter;
    }

    /// <summary>
    /// Twelve requests spread over three replicas against a budget of five. Exactly five are granted.
    /// Every refusal says when to retry, and that time falls inside the window.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestFixedWindowBudgetIsSharedAcrossReplicas(
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key = NewKey();

            // An hour-long window, so the test cannot straddle a window boundary.
            RateLimiter[] limiters = Replicas(kahuna1, kahuna2, kahuna3)
                .Select(client => (RateLimiter)new KahunaFixedWindowRateLimiter(client, new()
                {
                    Key = key,
                    Durability = durability,
                    PermitLimit = 5,
                    Window = TimeSpan.FromHours(1)
                }))
                .ToArray();

            int granted = 0;

            for (int i = 0; i < 12; i++)
            {
                RateLimitLease lease = await limiters[i % limiters.Length].AcquireAsync(1, Token);

                if (lease.IsAcquired)
                {
                    granted++;
                    continue;
                }

                TimeSpan retryAfter = RetryAfter(lease);
                Assert.InRange(retryAfter, TimeSpan.FromMilliseconds(1), TimeSpan.FromHours(1));
            }

            Assert.Equal(5, granted);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Twenty requests race over three replicas for a budget of five. Exactly five are granted.
    /// The decision is one transaction per request, so two racers can never both read the same count.
    /// </summary>
    [Fact]
    public async Task TestFixedWindowBudgetHoldsUnderContention()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key = NewKey();

            RateLimiter[] limiters = Replicas(kahuna1, kahuna2, kahuna3)
                .Select(client => (RateLimiter)new KahunaFixedWindowRateLimiter(client, new()
                {
                    Key = key,
                    PermitLimit = 5,
                    Window = TimeSpan.FromHours(1),
                    MaxRetries = 100
                }))
                .ToArray();

            (int granted, int refused, _) = await AcquireConcurrently(limiters, 20);

            Assert.Equal(5, granted);
            Assert.Equal(15, refused);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A full window refuses with the time left in the window, and after that time the budget is
    /// whole again.
    /// </summary>
    [Fact]
    public async Task TestFixedWindowResetsAfterTheRetryAfter()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);
            KahunaFixedWindowRateLimiterOptions options = new() { Key = NewKey(), PermitLimit = 2, Window = TimeSpan.FromSeconds(1) };

            KahunaFixedWindowRateLimiter first = new(clients[0], options);
            KahunaFixedWindowRateLimiter second = new(clients[1], options);

            // Spend the window, whichever window it is. A spent window refuses.
            RateLimitLease lease;

            do
                lease = await first.AcquireAsync(1, Token);
            while (lease.IsAcquired);

            TimeSpan retryAfter = RetryAfter(lease);
            Assert.InRange(retryAfter, TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1));

            await Task.Delay(retryAfter + TimeSpan.FromMilliseconds(50), Token);

            Assert.True((await second.AcquireAsync(1, Token)).IsAcquired);
            Assert.True((await first.AcquireAsync(1, Token)).IsAcquired);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The permits of the oldest segment come back when it leaves the window, and only those.
    ///
    /// Two permits are spent, and two more a little over two segments later, which fills the budget
    /// of four. The refusal names the moment the first two leave the window. At that moment two
    /// permits are free again, while the later two still count. A fixed window would give back all
    /// four at once, or none.
    /// </summary>
    [Fact]
    public async Task TestSlidingWindowReturnsPermitsSegmentBySegment()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);
            KahunaSlidingWindowRateLimiterOptions options = new()
            {
                Key = NewKey(),
                PermitLimit = 4,
                Window = TimeSpan.FromMilliseconds(2000),
                SegmentsPerWindow = 4
            };

            KahunaSlidingWindowRateLimiter first = new(clients[0], options);
            KahunaSlidingWindowRateLimiter second = new(clients[1], options);
            KahunaSlidingWindowRateLimiter third = new(clients[2], options);

            Assert.True((await first.AcquireAsync(2, Token)).IsAcquired);

            await Task.Delay(1100, Token);

            Assert.True((await second.AcquireAsync(2, Token)).IsAcquired);

            TimeSpan retryAfter = RetryAfter(await third.AcquireAsync(1, Token));
            Assert.InRange(retryAfter, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(1000));

            await Task.Delay(retryAfter + TimeSpan.FromMilliseconds(50), Token);

            Assert.True((await third.AcquireAsync(2, Token)).IsAcquired);

            // The later two permits are still in the window.
            TimeSpan stillHeld = RetryAfter(await first.AcquireAsync(1, Token));
            Assert.True(stillHeld > TimeSpan.Zero);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Twenty requests race over three replicas for a sliding window of five. Exactly five are granted.
    /// </summary>
    [Fact]
    public async Task TestSlidingWindowBudgetHoldsUnderContention()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key = NewKey();

            RateLimiter[] limiters = Replicas(kahuna1, kahuna2, kahuna3)
                .Select(client => (RateLimiter)new KahunaSlidingWindowRateLimiter(client, new()
                {
                    Key = key,
                    PermitLimit = 5,
                    Window = TimeSpan.FromHours(1),
                    SegmentsPerWindow = 10,
                    MaxRetries = 100
                }))
                .ToArray();

            (int granted, _, _) = await AcquireConcurrently(limiters, 20);

            Assert.Equal(5, granted);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A bucket starts full and allows a burst of its whole size. Once it is empty, the refusal names
    /// the time until the next token, and after that time exactly one more request is granted.
    /// </summary>
    [Fact]
    public async Task TestTokenBucketBurstsThenRefillsOnePeriodAtATime()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);
            KahunaTokenBucketRateLimiterOptions options = new()
            {
                Key = NewKey(),
                TokenLimit = 3,
                ReplenishmentPeriod = TimeSpan.FromMilliseconds(1000),
                TokensPerPeriod = 1
            };

            KahunaTokenBucketRateLimiter[] limiters = clients.Select(client => new KahunaTokenBucketRateLimiter(client, options)).ToArray();

            for (int i = 0; i < 3; i++)
                Assert.True((await limiters[i].AcquireAsync(1, Token)).IsAcquired);

            TimeSpan retryAfter = RetryAfter(await limiters[0].AcquireAsync(1, Token));
            Assert.InRange(retryAfter, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(1000));

            await Task.Delay(retryAfter + TimeSpan.FromMilliseconds(50), Token);

            Assert.True((await limiters[1].AcquireAsync(1, Token)).IsAcquired);
            Assert.False((await limiters[2].AcquireAsync(1, Token)).IsAcquired);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Twenty requests race over three replicas for a bucket of five tokens. Exactly five are granted.
    /// </summary>
    [Fact]
    public async Task TestTokenBucketHoldsUnderContention()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key = NewKey();

            RateLimiter[] limiters = Replicas(kahuna1, kahuna2, kahuna3)
                .Select(client => (RateLimiter)new KahunaTokenBucketRateLimiter(client, new()
                {
                    Key = key,
                    TokenLimit = 5,
                    ReplenishmentPeriod = TimeSpan.FromHours(1),
                    TokensPerPeriod = 1,
                    MaxRetries = 100
                }))
                .ToArray();

            (int granted, _, _) = await AcquireConcurrently(limiters, 20);

            Assert.Equal(5, granted);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Twelve requests race over three replicas for four concurrency permits. Exactly four are granted.
    /// Disposing one granted lease gives its permit back to any replica.
    /// </summary>
    [Fact]
    public async Task TestConcurrencyPermitsAreSharedAndComeBackOnDispose()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key = NewKey();

            KahunaConcurrencyLimiter[] limiters = Replicas(kahuna1, kahuna2, kahuna3)
                .Select(client => new KahunaConcurrencyLimiter(client, new() { Key = key, PermitLimit = 4, MaxRetries = 100 }))
                .ToArray();

            (int granted, _, List<RateLimitLease> leases) = await AcquireConcurrently(limiters, 12);

            Assert.Equal(4, granted);
            Assert.False((await limiters[0].AcquireAsync(1, Token)).IsAcquired);

            leases.First(lease => lease.IsAcquired).Dispose();

            // The release runs in the background. It is a single-key script, so it lands quickly.
            RateLimitLease next = await WaitForGrant(limiters[2]);
            Assert.True(next.IsAcquired);

            Assert.False((await limiters[1].AcquireAsync(1, Token)).IsAcquired);

            foreach (RateLimitLease lease in leases)
                lease.Dispose();

            next.Dispose();

            foreach (KahunaConcurrencyLimiter limiter in limiters)
                await limiter.DisposeAsync();
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A held permit outlives its lease duration while its holder runs, because the holder renews it.
    /// Once the holder stops renewing, here by disposing its limiter without disposing the lease, the
    /// permit comes back when the lease expires. That is what returns the permits of a dead process.
    /// </summary>
    [Fact]
    public async Task TestConcurrencyLeaseIsRenewedWhileHeldAndExpiresWhenAbandoned()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);
            KahunaConcurrencyLimiterOptions options = new()
            {
                Key = NewKey(),
                PermitLimit = 1,
                LeaseDuration = TimeSpan.FromMilliseconds(600)
            };

            KahunaConcurrencyLimiter holder = new(clients[0], options);
            KahunaConcurrencyLimiter other = new(clients[1], options);

            RateLimitLease held = await holder.AcquireAsync(1, Token);
            Assert.True(held.IsAcquired);

            // Three lease durations: without renewal the permit would have come back long ago.
            await Task.Delay(1800, Token);

            Assert.False((await other.AcquireAsync(1, Token)).IsAcquired);

            // The holder stops renewing, and its lease is never disposed.
            holder.Dispose();

            await Task.Delay(1200, Token);

            Assert.True((await other.AcquireAsync(1, Token)).IsAcquired);

            GC.KeepAlive(held);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A renewal and a release return no decision, so their outcome must not be parsed as one. A held
    /// lease is renewed many times and then released, and none of those calls may throw on the way.
    /// </summary>
    [Fact]
    public async Task TestConcurrencyRenewalAndReleaseThrowNothing()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        int unexpectedAnswers = 0;

        void OnFirstChance(object? sender, System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs e)
        {
            if (e.Exception is KahunaException ex && ex.Message.StartsWith("Rate limiter script returned an unexpected answer", StringComparison.Ordinal))
                Interlocked.Increment(ref unexpectedAnswers);
        }

        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);
            KahunaConcurrencyLimiterOptions options = new()
            {
                Key = NewKey(),
                PermitLimit = 1,
                LeaseDuration = TimeSpan.FromMilliseconds(300)
            };

            KahunaConcurrencyLimiter holder = new(clients[0], options);
            await using KahunaConcurrencyLimiter other = new(clients[1], options);

            RateLimitLease held = await holder.AcquireAsync(1, Token);
            Assert.True(held.IsAcquired);

            // Four lease durations, so about twelve renewals. The permit is still held only because
            // they took effect.
            await Task.Delay(1200, Token);
            Assert.False((await other.AcquireAsync(1, Token)).IsAcquired);

            held.Dispose();

            // An asynchronous disposal waits for the release that is still in flight.
            await holder.DisposeAsync();

            Assert.True((await other.AcquireAsync(1, Token)).IsAcquired);
            Assert.Equal(0, Volatile.Read(ref unexpectedAnswers));
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance;

            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A held lease whose key is gone from the cluster, as after an expiry, is dropped by its next
    /// renewal: the renewal does not write the key again, and the limiter stops counting the lease as
    /// held, so it can become idle.
    /// </summary>
    [Fact]
    public async Task TestConcurrencyLeaseThatLapsedIsDroppedByTheRenewal()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);
            string key = NewKey();

            await using KahunaConcurrencyLimiter limiter = new(clients[0], new()
            {
                Key = key,
                PermitLimit = 1,
                LeaseDuration = TimeSpan.FromMilliseconds(600)
            });

            RateLimitLease held = await limiter.AcquireAsync(1, Token);
            Assert.True(held.IsAcquired);
            Assert.Null(limiter.IdleDuration);

            List<KahunaKeyValue> leases = await clients[1].GetByBucket(key, KeyValueDurability.Ephemeral, cancellationToken: Token);
            KahunaKeyValue lease = Assert.Single(leases);

            // Another party removes the lease key, as its expiry would.
            await clients[1].ExecuteKeyValueTransactionScript("EDELETE @lease_key", parameters: [new() { Key = "@lease_key", Value = lease.Key }], cancellationToken: Token);

            await WaitUntil(() => limiter.IdleDuration is not null);

            // Two more renewal intervals: no renewal writes the key again.
            await Task.Delay(400, Token);
            Assert.Empty(await clients[1].GetByBucket(key, KeyValueDurability.Ephemeral, cancellationToken: Token));

            // Disposing the dropped lease has nothing left to give back.
            held.Dispose();
            Assert.True((await limiter.AcquireAsync(1, Token)).IsAcquired);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A request refused by a concurrency limiter waits in the local queue, and is granted as soon as
    /// this process gives a permit back. With the newest served first, a full queue evicts the oldest
    /// waiter with a refusal to make room.
    /// </summary>
    [Fact]
    public async Task TestConcurrencyQueueServesNewestFirstAndEvictsTheOldest()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);

            await using KahunaConcurrencyLimiter limiter = new(clients[0], new()
            {
                Key = NewKey(),
                PermitLimit = 1,
                QueueLimit = 1,
                QueueProcessingOrder = QueueProcessingOrder.NewestFirst,
                // Long enough that only the local release, not the poll, can serve the queue in time.
                QueuePollInterval = TimeSpan.FromSeconds(30)
            });

            RateLimitLease held = await limiter.AcquireAsync(1, Token);
            Assert.True(held.IsAcquired);

            Task<RateLimitLease> oldest = limiter.AcquireAsync(1, Token).AsTask();
            await WaitUntil(() => limiter.GetStatistics()!.CurrentQueuedCount == 1);

            Task<RateLimitLease> newest = limiter.AcquireAsync(1, Token).AsTask();

            RateLimitLease evicted = await oldest.WaitAsync(TimeSpan.FromSeconds(10), Token);
            Assert.False(evicted.IsAcquired);
            Assert.False(newest.IsCompleted);

            held.Dispose();

            RateLimitLease served = await newest.WaitAsync(TimeSpan.FromSeconds(10), Token);
            Assert.True(served.IsAcquired);

            served.Dispose();
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A request refused by a fixed window waits in the queue for the time the cluster named, and is
    /// granted when the next window opens. A queued request that is cancelled leaves the queue.
    /// </summary>
    [Fact]
    public async Task TestFixedWindowQueueWaitsForTheNextWindowAndHonoursCancellation()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            KahunaClient[] clients = Replicas(kahuna1, kahuna2, kahuna3);
            string key = NewKey();

            await using KahunaFixedWindowRateLimiter limiter = new(clients[0], new()
            {
                Key = key,
                PermitLimit = 1,
                Window = TimeSpan.FromSeconds(3),
                QueueLimit = 2
            });

            // Another replica, with no queue, spends the current window. A refusal from the queued
            // limiter would wait for the next window instead of returning.
            await using KahunaFixedWindowRateLimiter spender = new(clients[1], new()
            {
                Key = key,
                PermitLimit = 1,
                Window = TimeSpan.FromSeconds(3)
            });

            // Stop with at least a second left in the window, so the queued request below is refused
            // before the window rolls over.
            while (true)
            {
                RateLimitLease spent = await spender.AcquireAsync(1, Token);

                if (spent.IsAcquired)
                    continue;

                TimeSpan left = RetryAfter(spent);

                if (left >= TimeSpan.FromSeconds(1))
                    break;

                await Task.Delay(left + TimeSpan.FromMilliseconds(50), Token);
            }

            using CancellationTokenSource cancelled = CancellationTokenSource.CreateLinkedTokenSource(Token);

            Task<RateLimitLease> abandoned = limiter.AcquireAsync(1, cancelled.Token).AsTask();
            await WaitUntil(() => limiter.GetStatistics()!.CurrentQueuedCount == 1);

            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);
            Assert.Equal(0, limiter.GetStatistics()!.CurrentQueuedCount);

            RateLimitLease queued = await limiter.AcquireAsync(1, Token).AsTask().WaitAsync(TimeSpan.FromSeconds(10), Token);
            Assert.True(queued.IsAcquired);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The synchronous path never reaches the cluster, so it refuses. A request for more permits than
    /// the limit is a caller error, as it is for the framework's own limiters.
    /// </summary>
    [Fact]
    public void TestSynchronousAttemptRefusesAndOversizedRequestThrows()
    {
        KahunaClient client = new("http://localhost", communication: FailingCommunication.Create());

        using KahunaFixedWindowRateLimiter limiter = new(client, new() { Key = NewKey(), PermitLimit = 2, Window = TimeSpan.FromSeconds(1) });

        Assert.False(limiter.AttemptAcquire(1).IsAcquired);
        Assert.Throws<ArgumentOutOfRangeException>(() => limiter.AttemptAcquire(3));
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = limiter.AcquireAsync(3, Token).AsTask(); });
        Assert.Throws<ArgumentException>(() => new KahunaFixedWindowRateLimiter(client, new() { PermitLimit = 2, Window = TimeSpan.FromSeconds(1) }));
    }

    /// <summary>
    /// When the cluster cannot be reached, the failure mode decides: throw, admit, or refuse.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestFailureModeDecidesWhenTheClusterIsUnreachable(
        [CombinatorialValues(KahunaRateLimiterFailureMode.Throw, KahunaRateLimiterFailureMode.Allow, KahunaRateLimiterFailureMode.Deny)] KahunaRateLimiterFailureMode mode)
    {
        KahunaClient client = new("http://localhost", communication: FailingCommunication.Create());

        await using KahunaTokenBucketRateLimiter limiter = new(client, new()
        {
            Key = NewKey(),
            TokenLimit = 5,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            TokensPerPeriod = 1,
            FailureMode = mode
        });

        switch (mode)
        {
            case KahunaRateLimiterFailureMode.Throw:
                await Assert.ThrowsAsync<HttpRequestException>(() => limiter.AcquireAsync(1, Token).AsTask());
                break;

            case KahunaRateLimiterFailureMode.Allow:
                Assert.True((await limiter.AcquireAsync(1, Token)).IsAcquired);
                break;

            case KahunaRateLimiterFailureMode.Deny:
                Assert.False((await limiter.AcquireAsync(1, Token)).IsAcquired);
                break;
        }
    }

    /// <summary>
    /// The partition helper builds one limiter per partition, each over its own key, so two partitions
    /// keep two budgets, while two replicas of one partition share one.
    /// </summary>
    [Fact]
    public async Task TestPartitionsKeepSeparateBudgets()
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string policy = "policy-" + Guid.NewGuid().ToString("N")[..8];

            PartitionedRateLimiter<string>[] replicas = Replicas(kahuna1, kahuna2, kahuna3)
                .Select(client => PartitionedRateLimiter.Create<string, string>(user =>
                    KahunaRateLimitPartition.GetFixedWindowLimiter(client, user, key => new()
                    {
                        Key = KahunaRateLimitPartition.KeyFor(policy, key),
                        PermitLimit = 2,
                        Window = TimeSpan.FromHours(1)
                    })))
                .ToArray();

            int alice = 0;
            int bob = 0;

            for (int i = 0; i < 6; i++)
            {
                if ((await replicas[i % 3].AcquireAsync("alice", 1, Token)).IsAcquired)
                    alice++;

                if ((await replicas[(i + 1) % 3].AcquireAsync("bob", 1, Token)).IsAcquired)
                    bob++;
            }

            Assert.Equal(2, alice);
            Assert.Equal(2, bob);

            foreach (PartitionedRateLimiter<string> replica in replicas)
                await replica.DisposeAsync();
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// An ephemeral fixed-window or token-bucket decision reads and writes one key, so it runs inside
    /// one turn of the actor that owns the key instead of on the general transaction path. Each of
    /// these decisions must be counted as a completed turn. A script that left its turn is not
    /// counted, so the count also proves that none of them escaped.
    /// </summary>
    [Fact]
    public async Task TestFixedWindowAndTokenBucketDecideInsideOneActorTurn()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(Token);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        await using KahunaFixedWindowRateLimiter window = new(client, new() { Key = NewKey(), PermitLimit = 3, Window = TimeSpan.FromHours(1) });
        await using KahunaTokenBucketRateLimiter bucket = new(client, new() { Key = NewKey(), TokenLimit = 3, ReplenishmentPeriod = TimeSpan.FromHours(1), TokensPerPeriod = 1 });

        // Warm up: the first run of a script parses it, and the routing of a new key settles.
        await window.AcquireAsync(1, Token);
        await bucket.AcquireAsync(1, Token);

        long before = DurableTransactionMetrics.ScriptActorTurnsCount;

        // Grants and refusals both: each takes a different branch of the script.
        for (int i = 0; i < 5; i++)
        {
            await window.AcquireAsync(1, Token);
            await bucket.AcquireAsync(1, Token);
        }

        Assert.True(DurableTransactionMetrics.ScriptActorTurnsCount - before >= 10);
    }

    private static async Task<RateLimitLease> WaitForGrant(RateLimiter limiter)
    {
        for (int i = 0; i < 100; i++)
        {
            RateLimitLease lease = await limiter.AcquireAsync(1, Token);

            if (lease.IsAcquired)
                return lease;

            await Task.Delay(20, Token);
        }

        return await limiter.AcquireAsync(1, Token);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        for (int i = 0; i < 500 && !condition(); i++)
            await Task.Delay(10, Token);

        Assert.True(condition());
    }

    /// <summary>
    /// A transport for a cluster that cannot be reached: every call fails as a dropped connection would.
    /// </summary>
    public class FailingCommunication : DispatchProxy
    {
        public static IKahunaCommunication Create() => Create<IKahunaCommunication, FailingCommunication>();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            throw new HttpRequestException("Connection refused");
    }
}

# Distributed rate limiting guide

A rate limiter inside one process counts the requests of that process only. When an application
runs as several replicas, each replica keeps its own count. A limit of 100 requests per minute then
admits 100 requests per minute **per replica**, so five replicas admit 500.

The Kahuna rate limiters keep the count in Kahuna. Every replica that names the same key spends one
budget. The limiters are ordinary `System.Threading.RateLimiting.RateLimiter` classes, so they work
anywhere a .NET rate limiter works, and the ASP.NET Core middleware uses them through four extension
methods.

This guide is for a developer who adds rate limits to an application that uses a Kahuna cluster.

## 1. Packages

| Package | Contains | Needs |
| --- | --- | --- |
| `Kahuna.Client` | the four limiters and `KahunaRateLimitPartition` | nothing more than the client |
| `Kahuna.Client.AspNetCore` | the `AddKahuna…Limiter` methods on `RateLimiterOptions` | the ASP.NET Core shared framework |

The ASP.NET Core methods are in a separate package. A console or worker application that uses
`Kahuna.Client` then does not need the ASP.NET Core runtime.

## 2. The four limiters

| Limiter | Budget | Typical use |
| --- | --- | --- |
| `KahunaFixedWindowRateLimiter` | `PermitLimit` permits per window. The budget resets on each window boundary. | simple endpoints |
| `KahunaSlidingWindowRateLimiter` | `PermitLimit` permits in any window. A segment's permits come back when the segment leaves the window. | accurate limits with no burst at the boundary |
| `KahunaTokenBucketRateLimiter` | bursts up to `TokenLimit`, refilled by `TokensPerPeriod` every `ReplenishmentPeriod` | login attempts, checkout |
| `KahunaConcurrencyLimiter` | `PermitLimit` requests in progress at once. A permit comes back when its lease is disposed. | expensive operations |

Each limiter makes one decision in one script transaction. The script reads the state, decides, and
writes the state back. Two replicas that race for the last permit therefore cannot both get it.

Time comes from the cluster clock (`hlc()`), not from the replicas. Every replica agrees on where a
window starts and on how many tokens a bucket holds.

## 3. ASP.NET Core

Register a `KahunaClient` and add the policies:

```csharp
builder.Services.AddSingleton(new KahunaClient(["https://kahuna1:8082", "https://kahuna2:8084", "https://kahuna3:8086"]));

builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;

    options.OnRejected = (context, _) =>
    {
        if (context.Lease.TryGetMetadata(MetadataName.RetryAfter, out TimeSpan retryAfter))
            context.HttpContext.Response.Headers.RetryAfter = ((int)Math.Ceiling(retryAfter.TotalSeconds)).ToString();

        return ValueTask.CompletedTask;
    };

    // At most 10 expensive operations in progress across all replicas. Up to 5 more wait in each
    // replica's queue. When that queue is full, the oldest waiter is refused.
    options.AddKahunaConcurrencyLimiter("expensive_operations", o =>
    {
        o.PermitLimit = 10;
        o.QueueLimit = 5;
        o.QueueProcessingOrder = QueueProcessingOrder.NewestFirst;
    });

    // Bursts of 20, then 5 more every 10 seconds.
    options.AddKahunaTokenBucketLimiter("bursty_endpoints", o =>
    {
        o.TokenLimit = 20;
        o.ReplenishmentPeriod = TimeSpan.FromSeconds(10);
        o.TokensPerPeriod = 5;
    });

    // 100 requests per minute.
    options.AddKahunaFixedWindowLimiter("simple_endpoints", o =>
    {
        o.PermitLimit = 100;
        o.Window = TimeSpan.FromMinutes(1);
    });

    // 50 requests in any 5 minutes, tracked in 10 segments of 30 seconds.
    options.AddKahunaSlidingWindowLimiter("api_heavy", o =>
    {
        o.PermitLimit = 50;
        o.Window = TimeSpan.FromMinutes(5);
        o.SegmentsPerWindow = 10;
    });

    // 5 login attempts per email, then 1 more every 5 minutes.
    options.AddKahunaTokenBucketLimiter("login_attempts",
        context =>
        {
            string email = context.Request.Query["email"].ToString();
            return string.IsNullOrEmpty(email) ? "unknown" : email;
        },
        o =>
        {
            o.TokenLimit = 5;
            o.ReplenishmentPeriod = TimeSpan.FromMinutes(5);
            o.TokensPerPeriod = 1;
        });
});

app.UseRateLimiter();

app.MapPost("/reports", GenerateReport).RequireRateLimiting("expensive_operations");
app.MapPost("/login", Login).RequireRateLimiting("login_attempts");
```

Each method has two forms:

- Without `partitionBy`, the policy has one budget for all requests.
- With `partitionBy`, each partition has its own budget. The function returns the partition key
  for a request, for example a user id, an email or a client address.

The extension methods set these defaults:

- The Kahuna key of a policy is `rate-limit/<policy>`.
- The Kahuna key of a partition is `rate-limit/<policy>/<partition>`.
- The `configure` callback sees that key in `o.Key` and can replace it.
- The `configure` callback runs once when the policy is added, so a wrong setting fails at startup.
  It then runs once for each partition that the policy creates.
- The client is the `KahunaClient` in the request services. To use another client, pass it as the
  last argument.

The partition key becomes part of a Kahuna key. Anyone who can read the cluster can read it. If the
partition key is personal data, such as an email, hash it in `partitionBy`.

## 4. Without ASP.NET Core

Create a limiter directly:

```csharp
await using KahunaTokenBucketRateLimiter limiter = new(kahuna, new()
{
    Key = "rate-limit/outbound-email",
    TokenLimit = 20,
    ReplenishmentPeriod = TimeSpan.FromSeconds(10),
    TokensPerPeriod = 5
});

using RateLimitLease lease = await limiter.AcquireAsync(1, cancellationToken);

if (!lease.IsAcquired)
    return; // or wait, or report the RetryAfter metadata
```

For one budget per partition, use `KahunaRateLimitPartition` with
`PartitionedRateLimiter.Create`, or with the `AddPolicy` overloads of ASP.NET Core:

```csharp
PartitionedRateLimiter<string> perTenant = PartitionedRateLimiter.Create<string, string>(tenant =>
    KahunaRateLimitPartition.GetFixedWindowLimiter(kahuna, tenant, key => new()
    {
        Key = KahunaRateLimitPartition.KeyFor("tenant-api", key),
        PermitLimit = 1000,
        Window = TimeSpan.FromMinutes(1)
    }));
```

## 5. Settings that every limiter shares

| Setting | Default | Meaning |
| --- | --- | --- |
| `Key` | none | The Kahuna key that holds the state. Limiters that name one key share one budget. |
| `Durability` | `Ephemeral` | `Ephemeral` keeps the state in memory on the cluster. `Persistent` replicates and persists each decision, and costs much more. |
| `QueueLimit` | 0 | How many permits may wait in this process for the budget to free up. |
| `QueueProcessingOrder` | `OldestFirst` | `NewestFirst` also refuses the oldest waiters when the queue is full. |
| `FailureMode` | `Throw` | What to do when no decision is possible: `Throw`, `Allow` (admit) or `Deny` (refuse). |
| `MaxRetries` | 8 | How many times a decision that aborted on a contended key runs again. |

Give every replica that names one key the same limits. Each replica judges the shared state by its
own settings, so two replicas with different limits do not agree on anything.

## 6. Behavior to know

**The synchronous path refuses.** A decision needs a round trip to the cluster.
`RateLimiter.AttemptAcquire` must not block, so it always refuses. Use `AcquireAsync`. The ASP.NET
Core middleware calls `AttemptAcquire` first and then `AcquireAsync`, so it gets the real decision.

**A refusal says when to retry.** A refused lease of a window or bucket limiter carries
`MetadataName.RetryAfter`: the time until the budget can cover the request. A concurrency limiter
cannot know when another replica releases a permit, so its refusals carry no retry time.

**The queue is local.** `QueueLimit` and `QueueProcessingOrder` apply to the waiters of one process.
The budget is shared, but the replicas do not share one queue. A queued window or bucket request
sleeps for the retry time the cluster gave. A queued concurrency request asks again every
`QueuePollInterval` (50 ms by default), and at once when this process releases a permit.

**Concurrency permits are leases.** Each held permit is a Kahuna key under `<Key>/` that expires
after `LeaseDuration` (30 seconds by default). The holder renews it every third of that period until
the lease is disposed. The permits of a process that stops come back when its leases expire. Do not
store other keys directly under the `Key` of a concurrency limiter.

**A release runs in the background.** `RateLimitLease.Dispose` must not wait for the cluster, so a
concurrency limiter deletes the lease key in the background. If the delete fails after its retries,
the permit comes back when the lease expires.

**Idle partitions are disposed.** A partitioned limiter disposes a partition that stays idle. The
state stays in Kahuna, so a later request for that partition continues with the same budget.

**Cost.** With `Ephemeral` durability, a fixed-window or token-bucket decision reads and writes one
key. It uses the single-key script fast path, described in the single-key script fast path guide.
A sliding-window decision loops over its segments, and a concurrency decision reads a bucket of
keys. Both use the general transaction path, which costs more per decision.

using System.Net;
using System.Threading.RateLimiting;
using Kahuna.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Runs the Kahuna policies through the real ASP.NET Core rate-limiting middleware, in two
/// application replicas served by Kestrel, against one embedded Kahuna node.
///
/// The middleware asks a limiter synchronously first and only then asynchronously. A Kahuna limiter
/// refuses the synchronous attempt, because it cannot decide without the cluster, so these tests
/// prove the middleware still reaches the asynchronous decision and admits what the budget allows.
/// </summary>
public sealed class TestKahunaRateLimitingMiddleware
{
    private readonly ILoggerFactory loggerFactory;

    public TestKahunaRateLimitingMiddleware(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    /// <summary>
    /// The application under test: one fixed-window endpoint, one token-bucket endpoint partitioned
    /// by a query value, and one concurrency endpoint that signals when it starts and then holds its
    /// request until a gate opens. A refusal is answered with 429 and a Retry-After header taken from
    /// the lease.
    /// </summary>
    private static async Task<(WebApplication App, HttpClient Http)> StartReplica(KahunaClient kahuna, string prefix, TaskCompletionSource entered, TaskCompletionSource gate)
    {
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton(kahuna);

        builder.Services.AddRateLimiter(options =>
        {
            options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;

            options.OnRejected = (context, _) =>
            {
                if (context.Lease.TryGetMetadata(MetadataName.RetryAfter, out TimeSpan retryAfter))
                    context.HttpContext.Response.Headers.RetryAfter = ((int)Math.Ceiling(retryAfter.TotalSeconds)).ToString();

                return ValueTask.CompletedTask;
            };

            options.AddKahunaFixedWindowLimiter("simple_endpoints", o =>
            {
                o.Key = prefix + "/simple";
                o.PermitLimit = 3;
                o.Window = TimeSpan.FromHours(1);
            });

            options.AddKahunaTokenBucketLimiter("login_attempts",
                context => context.Request.Query["email"].ToString(),
                o =>
                {
                    o.Key = prefix + "/login/" + o.Key;
                    o.TokenLimit = 2;
                    o.ReplenishmentPeriod = TimeSpan.FromHours(1);
                    o.TokensPerPeriod = 1;
                });

            options.AddKahunaConcurrencyLimiter("expensive_operations", o =>
            {
                o.Key = prefix + "/expensive";
                o.PermitLimit = 1;
            });
        });

        WebApplication app = builder.Build();

        app.UseRateLimiter();

        app.MapGet("/simple", () => "ok").RequireRateLimiting("simple_endpoints");
        app.MapGet("/login", () => "ok").RequireRateLimiting("login_attempts");
        app.MapGet("/expensive", async () =>
        {
            entered.TrySetResult();
            await gate.Task;
            return "ok";
        }).RequireRateLimiting("expensive_operations");

        await app.StartAsync(Token);

        return (app, new() { BaseAddress = new(app.Urls.First()) });
    }

    [Fact]
    public async Task TestReplicasShareBudgetsThroughTheMiddleware()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(Token);

        KahunaClient kahuna = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));
        string prefix = "rate-limit/web/" + Guid.NewGuid().ToString("N")[..8];
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        (WebApplication app1, HttpClient http1) = await StartReplica(kahuna, prefix, entered, gate);
        (WebApplication app2, HttpClient http2) = await StartReplica(kahuna, prefix, entered, gate);

        try
        {
            // Fixed window: three requests per window, spread over two replicas.
            List<HttpResponseMessage> simple = [];

            for (int i = 0; i < 6; i++)
                simple.Add(await (i % 2 == 0 ? http1 : http2).GetAsync("/simple", Token));

            Assert.Equal(3, simple.Count(r => r.StatusCode == HttpStatusCode.OK));
            Assert.Equal(3, simple.Count(r => r.StatusCode == HttpStatusCode.TooManyRequests));
            Assert.All(simple.Where(r => r.StatusCode == HttpStatusCode.TooManyRequests), r => Assert.True(r.Headers.RetryAfter is not null));

            // Token bucket per email: each email has its own two tokens.
            int alice = 0;
            int bob = 0;

            for (int i = 0; i < 4; i++)
            {
                if ((await http1.GetAsync("/login?email=alice@example.com", Token)).StatusCode == HttpStatusCode.OK)
                    alice++;

                if ((await http2.GetAsync("/login?email=bob@example.com", Token)).StatusCode == HttpStatusCode.OK)
                    bob++;
            }

            Assert.Equal(2, alice);
            Assert.Equal(2, bob);

            // Concurrency: one request in progress across both replicas. The first holds the permit
            // until the gate opens, so a second one on the other replica is refused.
            Task<HttpResponseMessage> holding = http1.GetAsync("/expensive", Token);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), Token);

            HttpResponseMessage refused = await http2.GetAsync("/expensive", Token).WaitAsync(TimeSpan.FromSeconds(10), Token);
            Assert.Equal(HttpStatusCode.TooManyRequests, refused.StatusCode);

            gate.SetResult();
            Assert.Equal(HttpStatusCode.OK, (await holding).StatusCode);

            // The permit comes back when the first request ends, so the other replica gets it.
            HttpResponseMessage after = new(HttpStatusCode.TooManyRequests);

            for (int i = 0; i < 100 && after.StatusCode != HttpStatusCode.OK; i++)
            {
                after = await http2.GetAsync("/expensive", Token);

                if (after.StatusCode != HttpStatusCode.OK)
                    await Task.Delay(20, Token);
            }

            Assert.Equal(HttpStatusCode.OK, after.StatusCode);
        }
        finally
        {
            gate.TrySetResult();

            await app1.StopAsync(Token);
            await app2.StopAsync(Token);
            await app1.DisposeAsync();
            await app2.DisposeAsync();
        }
    }
}

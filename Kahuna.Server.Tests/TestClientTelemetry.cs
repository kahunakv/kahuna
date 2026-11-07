/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Client.Diagnostics;
using Kahuna.Client.Routing;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.DependencyInjection;

namespace Kahuna.Server.Tests;

/// <summary>
/// Runs alone: the activity source and the histogram are process-wide, so a client operation that runs
/// in a parallel test would add spans and samples while these tests count. Running alone does not stop
/// an operation that an earlier test left in the background, so each collector also keeps only the
/// operations of its own test.
/// </summary>
[CollectionDefinition("KahunaClientTelemetry", DisableParallelization = true)]
public sealed class KahunaClientTelemetryCollection;

/// <summary>
/// A transport whose every method is answered by <see cref="Handler"/>. It stands in for the
/// network, so the tests see exactly what the instrumented layer adds and nothing a real node does.
/// </summary>
public class ScriptedCommunication : DispatchProxy
{
    public Func<MethodInfo, object?[]?, object?> Handler { get; set; } =
        static (method, _) => throw new NotSupportedException(method.Name);

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Handler(targetMethod!, args);

    public static IKahunaCommunication Create(Func<MethodInfo, object?[]?, object?> handler)
    {
        IKahunaCommunication proxy = Create<IKahunaCommunication, ScriptedCommunication>();
        ((ScriptedCommunication)(object)proxy).Handler = handler;
        return proxy;
    }
}

[Collection("KahunaClientTelemetry")]
public sealed class TestClientTelemetry
{
    private const string Url = "http://kahuna-node:8083";

    private const string ParentSourceName = "Kahuna.Server.Tests.ClientTelemetry";

    private static readonly ActivitySource ParentSource = new(ParentSourceName);

    private static readonly HLCTimestamp NoTransaction = HLCTimestamp.Zero;

    /// <summary>Collects the client spans of one trace, so a span from any other trace is ignored.</summary>
    private sealed class SpanCollector : IDisposable
    {
        private readonly ActivityListener listener;

        private readonly ConcurrentQueue<Activity> stopped = new();

        public SpanCollector()
        {
            listener = new()
            {
                ShouldListenTo = source => source.Name is KahunaClientTelemetry.ActivitySourceName or ParentSourceName,
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    if (activity.Source.Name == KahunaClientTelemetry.ActivitySourceName)
                        stopped.Enqueue(activity);
                }
            };

            ActivitySource.AddActivityListener(listener);
        }

        public List<Activity> SpansOf(Activity parent) =>
            stopped.Where(a => a.TraceId == parent.TraceId).ToList();

        public void Dispose() => listener.Dispose();
    }

    /// <summary>
    /// Collects the duration samples that the creating test's own async flow records, so a sample from
    /// an operation on any other flow is ignored. The histogram call runs on the flow of the operation.
    /// </summary>
    private sealed class DurationCollector : IDisposable
    {
        private static readonly AsyncLocal<DurationCollector?> Owner = new();

        private readonly MeterListener listener = new();

        public ConcurrentQueue<(double Value, Dictionary<string, object?> Tags)> Samples { get; } = new();

        public DurationCollector()
        {
            Owner.Value = this;

            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == KahunaClientTelemetry.MeterName &&
                    instrument.Name == KahunaClientTelemetry.OperationDurationName)
                    l.EnableMeasurementEvents(instrument);
            };

            listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            {
                if (!ReferenceEquals(Owner.Value, this))
                    return;

                Dictionary<string, object?> copy = new(tags.Length);

                foreach (KeyValuePair<string, object?> tag in tags)
                    copy[tag.Key] = tag.Value;

                Samples.Enqueue((value, copy));
            });

            listener.Start();
        }

        public void Dispose() => listener.Dispose();
    }

    private static IKahunaCommunication SetSucceeds() => ScriptedCommunication.Create((method, _) =>
        method.Name == nameof(IKahunaCommunication.TrySetKeyValue)
            ? Task.FromResult((true, 7L, 0))
            : throw new NotSupportedException(method.Name));

    private static Task<(bool, long, int)> Set(IKahunaCommunication communication) =>
        communication.TrySetKeyValue(Url, NoTransaction, "k", [1], 0, KeyValueFlags.Set, KeyValueDurability.Persistent, CancellationToken.None);

    [Fact]
    public async Task OperationIsOneClientSpanUnderTheAmbientActivity()
    {
        using SpanCollector spans = new();
        using Activity parent = ParentSource.StartActivity("request")!;

        InstrumentedKahunaCommunication communication = new(ScriptedCommunication.Create((method, _) =>
            Task.Run(async () =>
            {
                await Task.Yield();
                return (true, 7L, 0);
            })));

        Task<(bool, long, int)> pending = Set(communication);

        // The transport ran inside the span, but the caller must get its own ambient activity back
        // as soon as the call returns, not when the task completes.
        Assert.Same(parent, Activity.Current);

        (bool success, long revision, _) = await pending;

        Assert.True(success);
        Assert.Equal(7, revision);
        Assert.Same(parent, Activity.Current);

        Activity span = Assert.Single(spans.SpansOf(parent));
        Assert.Equal("SetKeyValue", span.DisplayName);
        Assert.Equal(ActivityKind.Client, span.Kind);
        Assert.Equal(parent.SpanId, span.ParentSpanId);
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        Assert.Equal("kahuna", span.GetTagItem("db.system.name"));
        Assert.Equal("SetKeyValue", span.GetTagItem("db.operation.name"));
        Assert.Equal("kahuna-node", span.GetTagItem("server.address"));
        Assert.Equal(8083, span.GetTagItem("server.port"));
        Assert.Null(span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task TransportRunsWithTheSpanAsItsAmbientActivity()
    {
        using SpanCollector spans = new();
        using Activity parent = ParentSource.StartActivity("request")!;

        Activity? seenByTransport = null;

        InstrumentedKahunaCommunication communication = new(ScriptedCommunication.Create((_, _) =>
        {
            seenByTransport = Activity.Current;
            return Task.FromResult((true, 1L, 0));
        }));

        await Set(communication);

        Activity span = Assert.Single(spans.SpansOf(parent));
        Assert.Same(span, seenByTransport);
    }

    [Fact]
    public async Task FailedOperationMarksTheSpanAndKeepsTheException()
    {
        using SpanCollector spans = new();
        using Activity parent = ParentSource.StartActivity("request")!;

        InstrumentedKahunaCommunication communication = new(ScriptedCommunication.Create((_, _) =>
            Task.FromException<(bool, long, int)>(new KahunaException("refused", KeyValueResponseType.Aborted))));

        KahunaException thrown = await Assert.ThrowsAsync<KahunaException>(() => Set(communication));

        Assert.Equal(KeyValueResponseType.Aborted, thrown.KeyValueErrorCode);
        Assert.Same(parent, Activity.Current);

        Activity span = Assert.Single(spans.SpansOf(parent));
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(nameof(KeyValueResponseType.Aborted), span.GetTagItem("error.type"));
        Assert.Contains(span.Events, e => e.Name == "exception");
    }

    [Fact]
    public async Task TransportThatThrowsBeforeReturningATaskStillEndsTheSpan()
    {
        using SpanCollector spans = new();
        using Activity parent = ParentSource.StartActivity("request")!;

        InstrumentedKahunaCommunication communication = new(ScriptedCommunication.Create((_, _) =>
            throw new InvalidOperationException("no channel")));

        // DispatchProxy wraps an exception thrown by the handler.
        await Assert.ThrowsAnyAsync<Exception>(() => Set(communication));

        Assert.Same(parent, Activity.Current);

        Activity span = Assert.Single(spans.SpansOf(parent));
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.NotNull(span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task EveryOperationRecordsOneDurationSample()
    {
        using DurationCollector durations = new();

        InstrumentedKahunaCommunication succeeding = new(SetSucceeds());

        InstrumentedKahunaCommunication failing = new(ScriptedCommunication.Create((_, _) =>
            Task.FromException<(bool, long, int)>(new KahunaException("refused", KeyValueResponseType.MustRetry))));

        await Set(succeeding);
        await Assert.ThrowsAsync<KahunaException>(() => Set(failing));

        Assert.Equal(2, durations.Samples.Count);

        (double okValue, Dictionary<string, object?> okTags) = durations.Samples.First();
        Assert.True(okValue >= 0);
        Assert.Equal("kahuna", okTags["db.system.name"]);
        Assert.Equal("SetKeyValue", okTags["db.operation.name"]);
        Assert.False(okTags.ContainsKey("error.type"));

        // Endpoints are not a dimension: one per node would multiply every series by the cluster size.
        Assert.False(okTags.ContainsKey("server.address"));

        Dictionary<string, object?> failedTags = durations.Samples.Last().Tags;
        Assert.Equal(nameof(KeyValueResponseType.MustRetry), failedTags["error.type"]);
    }

    [Fact]
    public void WithoutListenersTheTransportTaskIsReturnedUnchanged()
    {
        Assert.False(KahunaClientTelemetry.IsEnabled);

        Task<(bool, long, int)> transportTask = Task.FromResult((true, 3L, 0));

        InstrumentedKahunaCommunication communication = new(ScriptedCommunication.Create((_, _) => transportTask));

        Activity? before = Activity.Current;

        Assert.Same(transportTask, Set(communication));
        Assert.Same(before, Activity.Current);
    }

    [Fact]
    public async Task ClientOperationsAreTracedEndToEnd()
    {
        using SpanCollector spans = new();
        using Activity parent = ParentSource.StartActivity("request")!;

        KahunaClient client = new(Url, communication: SetSucceeds());

        KahunaKeyValue result = await client.SetKeyValue("k", [1], 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.True(result.Success);

        Activity span = Assert.Single(spans.SpansOf(parent));
        Assert.Equal("SetKeyValue", span.DisplayName);
    }

    [Fact]
    public void AddKahunaClientRegistersOneSingleton()
    {
        ServiceCollection services = new();
        services.AddLogging();
        services.AddKahunaClient(["http://a:8083", "http://b:8083"], options => options.Routing = KahunaRoutingMode.RoundRobin);

        using ServiceProvider provider = services.BuildServiceProvider();

        KahunaClient first = provider.GetRequiredService<KahunaClient>();

        Assert.Same(first, provider.GetRequiredService<KahunaClient>());
        Assert.Equal(KahunaRoutingMode.RoundRobin, first.EffectiveRouting);
    }

    [Fact]
    public void AddKeyedKahunaClientKeepsClustersApart()
    {
        ServiceCollection services = new();
        services.AddKeyedKahunaClient("orders", ["http://orders:8083"]);
        services.AddKeyedKahunaClient("sessions", ["http://sessions-1:8083", "http://sessions-2:8083"]);

        using ServiceProvider provider = services.BuildServiceProvider();

        KahunaClient orders = provider.GetRequiredKeyedService<KahunaClient>("orders");
        KahunaClient sessions = provider.GetRequiredKeyedService<KahunaClient>("sessions");

        Assert.NotSame(orders, sessions);
        Assert.Equal(KahunaRoutingMode.RoundRobin, orders.EffectiveRouting);
        Assert.Equal(KahunaRoutingMode.Learned, sessions.EffectiveRouting);
        Assert.Null(provider.GetService<KahunaClient>());
    }

    [Fact]
    public void AddKahunaClientRejectsAnEmptyEndpointList()
    {
        ServiceCollection services = new();

        Assert.Throws<ArgumentException>(() => services.AddKahunaClient(Array.Empty<string>()));
        Assert.Throws<ArgumentException>(() => services.AddKahunaClient(["http://a:8083", " "]));
    }
}

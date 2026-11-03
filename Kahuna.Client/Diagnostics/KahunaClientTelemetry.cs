/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Grpc.Core;
using Kahuna.Client.Routing;

namespace Kahuna.Client.Diagnostics;

/// <summary>
/// Names of the activity source and the meters that <see cref="KahunaClient"/> publishes. Pass them to
/// an OpenTelemetry tracer provider (<c>AddSource</c>) and meter provider (<c>AddMeter</c>) to export
/// one client span and one duration sample for every operation the client sends to a node.
/// <para>
/// Nothing is created when nothing listens: with no tracer provider and no meter listener attached,
/// an operation goes straight to the transport.
/// </para>
/// </summary>
public static class KahunaClientTelemetry
{
    /// <summary>The <see cref="System.Diagnostics.ActivitySource"/> name for client operation spans.</summary>
    public const string ActivitySourceName = "Kahuna.Client";

    /// <summary>The <see cref="System.Diagnostics.Metrics.Meter"/> name for the operation-duration histogram.</summary>
    public const string MeterName = "Kahuna.Client";

    /// <summary>The meter name for the routing-cache counters.</summary>
    public const string RoutingMeterName = RoutingMetrics.MeterName;

    /// <summary>The value of the <c>db.system.name</c> tag on every span and sample.</summary>
    public const string DbSystemName = "kahuna";

    internal const string DbSystemNameTag = "db.system.name";

    internal const string DbOperationNameTag = "db.operation.name";

    internal const string ServerAddressTag = "server.address";

    internal const string ServerPortTag = "server.port";

    internal const string ErrorTypeTag = "error.type";

    internal const string OperationDurationName = "kahuna.client.operation.duration";

    private static readonly string Version =
        typeof(KahunaClientTelemetry).Assembly.GetName().Version?.ToString() ?? "1.0.0";

    internal static readonly ActivitySource Source = new(ActivitySourceName, Version);

    private static readonly Meter Meter = new(MeterName, Version);

    // The boundaries the OpenTelemetry database conventions recommend for operation durations. An
    // exporter that ignores advice falls back to its own defaults, which are in milliseconds and too
    // coarse for sub-millisecond reads.
    internal static readonly Histogram<double> OperationDuration = Meter.CreateHistogram(
        OperationDurationName,
        unit: "s",
        description: "Duration of one operation that the Kahuna client sent to a node.",
        tags: null,
        advice: new InstrumentAdvice<double>
        {
            HistogramBucketBoundaries = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
        });

    /// <summary>
    /// True when a tracer provider listens to <see cref="Source"/> or a meter listener records
    /// <see cref="OperationDuration"/>. Read on every operation; both checks are a field read.
    /// </summary>
    internal static bool IsEnabled => Source.HasListeners() || OperationDuration.Enabled;

    // Endpoint URLs come from the configured list, the endpoint map, and accepted routing hints, so the
    // set is small. The cap only stops a misbehaving deployment from growing the cache without limit;
    // past it, a URL is parsed on each operation instead.
    private const int MaxCachedEndpoints = 256;

    private static readonly ConcurrentDictionary<string, (string Host, int Port)> Endpoints = new(StringComparer.Ordinal);

    internal static (string Host, int Port) GetEndpoint(string url)
    {
        if (Endpoints.TryGetValue(url, out (string Host, int Port) endpoint))
            return endpoint;

        endpoint = Uri.TryCreate(url, UriKind.Absolute, out Uri? uri) ? (uri.Host, uri.Port) : (url, 0);

        if (Endpoints.Count < MaxCachedEndpoints)
            Endpoints.TryAdd(url, endpoint);

        return endpoint;
    }

    /// <summary>
    /// A low-cardinality name for a failure: the Kahuna response code for a <see cref="KahunaException"/>,
    /// the gRPC status for an <see cref="RpcException"/>, and the exception type for anything else.
    /// </summary>
    internal static string GetErrorType(Exception exception) => exception switch
    {
        KahunaException kahuna => kahuna.ErrorCodeName,
        RpcException rpc => rpc.StatusCode.ToString(),
        OperationCanceledException => "cancelled",
        _ => exception.GetType().FullName ?? exception.GetType().Name
    };
}

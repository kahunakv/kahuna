/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Kahuna.Server.Diagnostics;

/// <summary>
/// Exports the node's metrics, request spans and logs over OTLP, only when the standard
/// <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> variable names a collector. A node started without it is
/// unchanged: no SDK, no exporter, no extra listener on the meters.
/// <para>
/// Every other exporter setting comes from the standard <c>OTEL_*</c> variables (protocol, headers,
/// service name, resource attributes), so an orchestrator such as .NET Aspire that sets them gets the
/// node into its dashboard with no Kahuna-specific flag.
/// </para>
/// </summary>
public static class OtlpTelemetry
{
    public const string EndpointVariable = "OTEL_EXPORTER_OTLP_ENDPOINT";

    public const string ServiceNameVariable = "OTEL_SERVICE_NAME";

    /// <summary>The service name used when <c>OTEL_SERVICE_NAME</c> is not set.</summary>
    public const string DefaultServiceName = "kahuna";

    public const string NodeNameAttribute = "kahuna.node.name";

    /// <summary>The .NET runtime meter: GC, thread pool, lock contention, exceptions.</summary>
    private const string RuntimeMeterName = "System.Runtime";

    /// <summary>Returns true when export was configured.</summary>
    public static bool AddIfConfigured(WebApplicationBuilder builder, string nodeName)
    {
        if (string.IsNullOrWhiteSpace(builder.Configuration[EndpointVariable]))
            return false;

        // An explicit service name would override OTEL_SERVICE_NAME, so it is set only as a fallback.
        bool hasServiceName = !string.IsNullOrWhiteSpace(builder.Configuration[ServiceNameVariable]);

        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeFormattedMessage = true;
            logging.IncludeScopes = true;
        });

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource =>
            {
                if (!hasServiceName)
                    resource.AddService(DefaultServiceName, serviceInstanceId: nodeName, autoGenerateServiceInstanceId: false);

                resource.AddAttributes([new(NodeNameAttribute, nodeName)]);
            })
            .WithMetrics(metrics => metrics
                .AddMeter(EngineMetricsCollector.KahunaMeterName, EngineMetricsCollector.KommanderMeterName, RuntimeMeterName)
                .AddAspNetCoreInstrumentation())
            .WithTracing(tracing => tracing
                .AddAspNetCoreInstrumentation(options => options.Filter = context => IsTracedPath(context.Request.Path.Value)))
            .UseOtlpExporter();

        return true;
    }

    /// <summary>
    /// Whether a request to <paramref name="path"/> becomes a span. Excluded are the requests whose
    /// span would be noise or would last as long as a connection:
    /// <list type="bullet">
    /// <item>Raft traffic between nodes (REST <c>/v1/raft/*</c> and gRPC <c>/Rafter/*</c>): a ping, a
    /// vote or an append on every heartbeat of every partition.</item>
    /// <item>The batch streams (<c>/Locker/Batch*</c>, <c>/KeyValuer/Batch*</c>) and the watch stream:
    /// one call carries many operations and lives as long as the connection. The client traces each
    /// operation in it instead.</item>
    /// <item>Health probes and the operator dashboard, which poll.</item>
    /// </list>
    /// </summary>
    public static bool IsTracedPath(string? path)
    {
        if (string.IsNullOrEmpty(path))
            return true;

        ReadOnlySpan<char> p = path.AsSpan();

        if (p.StartsWith("/v1/raft", StringComparison.Ordinal) ||
            p.StartsWith("/Rafter/", StringComparison.Ordinal) ||
            p.StartsWith("/v1/cluster/health", StringComparison.Ordinal) ||
            p.StartsWith("/v1/dashboard", StringComparison.Ordinal) ||
            p.SequenceEqual("/KeyValuer/Watch"))
            return false;

        int method = p.LastIndexOf('/');

        return method < 0 || !p[(method + 1)..].StartsWith("Batch", StringComparison.Ordinal);
    }
}

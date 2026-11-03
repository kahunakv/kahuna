/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Server.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Kahuna.Server.Tests;

public sealed class TestOtlpTelemetry
{
    [Theory]
    [InlineData("/v1/raft/ping")]
    [InlineData("/v1/raft/append-logs")]
    [InlineData("/Rafter/AppendLogs")]
    [InlineData("/Locker/BatchClientLockRequests")]
    [InlineData("/KeyValuer/BatchClientKeyValueRequests")]
    [InlineData("/KeyValuer/BatchServerKeyValueRequests")]
    [InlineData("/KeyValuer/Watch")]
    [InlineData("/v1/cluster/health")]
    [InlineData("/v1/dashboard/metrics")]
    public void ConnectionLongAndPollingRequestsAreNotTraced(string path) =>
        Assert.False(OtlpTelemetry.IsTracedPath(path));

    [Theory]
    [InlineData("/KeyValuer/TrySetKeyValue")]
    [InlineData("/KeyValuer/GetByRangeStream")]
    [InlineData("/Locker/TryLock")]
    [InlineData("/Sequencer/NextSequenceValue")]
    [InlineData("/Backups/TakeFullBackup")]
    [InlineData("/v1/kv/try-set")]
    [InlineData("/v1/cluster/placement")]
    [InlineData("/")]
    [InlineData("")]
    public void OperationRequestsAreTraced(string path) =>
        Assert.True(OtlpTelemetry.IsTracedPath(path));

    [Fact]
    public void NothingIsRegisteredWithoutAnEndpoint()
    {
        WebApplicationBuilder builder = WebApplication.CreateEmptyBuilder(new());

        Assert.False(OtlpTelemetry.AddIfConfigured(builder, "node1"));
        Assert.DoesNotContain(builder.Services, d => d.ServiceType == typeof(TracerProvider));
        Assert.DoesNotContain(builder.Services, d => d.ServiceType == typeof(MeterProvider));
    }

    [Fact]
    public void AnEndpointRegistersTracingAndMetrics()
    {
        WebApplicationBuilder builder = WebApplication.CreateEmptyBuilder(new());
        builder.Configuration[OtlpTelemetry.EndpointVariable] = "http://localhost:4317";

        Assert.True(OtlpTelemetry.AddIfConfigured(builder, "node1"));

        using ServiceProvider provider = builder.Services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<TracerProvider>());
        Assert.NotNull(provider.GetService<MeterProvider>());
    }
}

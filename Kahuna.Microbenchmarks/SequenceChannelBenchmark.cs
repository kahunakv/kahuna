using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the sequence-client channel reuse in
/// <c>GrpcCommunication</c> sequence methods (<c>GetSequence</c>/<c>NextSequenceValue</c>/...).
///
/// <para><b>Old</b> = a fresh <see cref="SocketsHttpHandler"/> + service-config + retry policy +
/// <see cref="GrpcChannel"/> built and disposed on every sequence call (the deleted
/// <c>CreateSequenceChannel</c>). <b>New</b> = pull a process-cached channel from the shared pool and
/// reuse it; nothing is constructed or disposed per call.</para>
///
/// <para>This isolates the local construct/dispose overhead without a cluster — neither path connects,
/// since <see cref="GrpcChannel.ForAddress(string, GrpcChannelOptions)"/> is lazy. The old code did not
/// even dispose the handler (<see cref="GrpcChannel"/> does not own a caller-supplied handler), so the
/// real per-call cost was strictly worse than measured here, plus a handler/connection leak; this
/// benchmark disposes the handler to keep the harness from accumulating native resources.</para>
///
/// To keep the channel observable (and not elided), each method reads <see cref="GrpcChannel.Target"/>.
///
/// <para><b>Result</b> (net10.0, Release): <c>NewChannelPerCall</c> ≈ 3,307 ns and 7,592 B/op;
/// <c>ReuseSharedChannel</c> ≈ 0 ns and 0 B/op. The reuse path removes the entire per-call
/// construct/dispose cost. This managed figure excludes the TLS/HTTP2 connection setup that the
/// per-call handler also forces on first real use against a live cluster, so the production win is
/// larger than the number above.</para>
/// </summary>
[MemoryDiagnoser]
public class SequenceChannelBenchmark
{
    private const string Url = "https://localhost:5001";

    private GrpcChannel _shared = null!;

    [GlobalSetup]
    public void Setup() => _shared = CreateChannel(Url);

    [GlobalCleanup]
    public void Cleanup() => _shared.Dispose();

    [Benchmark(Baseline = true)]
    public int NewChannelPerCall()
    {
        SocketsHttpHandler handler = BuildHandler();
        GrpcChannel channel = GrpcChannel.ForAddress(Url, BuildOptions(handler));
        int result = channel.Target.Length;
        channel.Dispose();
        handler.Dispose();
        return result;
    }

    [Benchmark]
    public int ReuseSharedChannel()
    {
        GrpcChannel channel = _shared;
        return channel.Target.Length;
    }

    private static GrpcChannel CreateChannel(string url)
    {
        SocketsHttpHandler handler = BuildHandler();
        return GrpcChannel.ForAddress(url, BuildOptions(handler));
    }

    private static SocketsHttpHandler BuildHandler() => new()
    {
        ConnectTimeout = TimeSpan.FromSeconds(10),
        PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
        KeepAlivePingDelay = TimeSpan.FromSeconds(30),
        KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
        EnableMultipleHttp2Connections = true,
    };

    private static GrpcChannelOptions BuildOptions(SocketsHttpHandler handler)
    {
        MethodConfig methodConfig = new()
        {
            Names = { MethodName.Default },
            RetryPolicy = new RetryPolicy
            {
                MaxAttempts = 5,
                InitialBackoff = TimeSpan.FromSeconds(1),
                MaxBackoff = TimeSpan.FromSeconds(5),
                BackoffMultiplier = 1.5,
                RetryableStatusCodes = { StatusCode.Unavailable }
            }
        };

        return new GrpcChannelOptions
        {
            HttpHandler = handler,
            ServiceConfig = new ServiceConfig { MethodConfigs = { methodConfig } }
        };
    }
}

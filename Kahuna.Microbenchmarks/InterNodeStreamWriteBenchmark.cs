using System.Net;

using BenchmarkDotNet.Attributes;

using Google.Protobuf;

using Grpc.Core;
using Grpc.Net.Client;

using Kahuna.Shared.Communication.Grpc;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Measures the transport leg of the inter-node batcher over real gRPC (Kestrel, HTTP/2,
/// loopback) with an echo service, so the four candidate transport shapes are comparable
/// without Raft, actors or persistence behind them.
///
/// <para>The production path in <c>GrpcServerBatcher</c> drains its inbox into one randomly
/// chosen stream and issues one awaited, semaphore-serialized gRPC message write per logical
/// operation. The four arms hold the logical work constant (1024 set operations per invocation)
/// and vary only the transport shape:</para>
/// <list type="bullet">
///   <item><b>OneStreamPerOpWrites</b> — the current shape: one duplex stream, one writer,
///     one envelope per operation.</item>
///   <item><b>OneStreamCoalesced16</b> — one stream, one writer, 16 operations per message.
///     The existing set-many payload stands in for the proposed multi-operation envelope; the
///     items carry the same key/value/flags fields as the single-op message, so the bytes are
///     nearly identical and only the message count changes (1024 → 64).</item>
///   <item><b>FourStreamsPerOpWrites</b> — four duplex streams with four independent writers,
///     one envelope per operation, operations split evenly.</item>
///   <item><b>FourStreamsCoalesced16</b> — both changes together.</item>
/// </list>
///
/// <para>An invocation completes when the client has received an echoed per-operation result for
/// every operation, so the measured time covers build, write, server read, server reply build,
/// reply write and client read — the full wire round trip, minus real request execution and minus
/// a real network. Loopback makes per-message CPU overhead maximally visible relative to total
/// time; a real network adds latency that is identical across arms and dilutes the difference, so
/// a small win here bounds the win available in a cluster from above.</para>
///
/// <para><b>Result</b> (net10.0 Release, .NET 10.0.10, Apple M4 Arm64 RyuJIT, concurrent server
/// GC; BenchmarkDotNet 0.15.8, 8 cases in 1m35s). Time and allocations per 1024 operations,
/// relative to the one-stream per-operation baseline:</para>
/// <list type="bullet">
///   <item>128-byte values: baseline 8.75 ms (8.5 µs per operation, ~117k operations/s on one
///     stream). Coalesced 16 on one stream: 0.75 ms (0.09× time, 0.33× allocations). Four
///     writers, per-operation envelopes: 3.67 ms (0.42× time, 1.04× allocations). Both
///     together: 0.36 ms (0.04× time, 0.33× allocations).</item>
///   <item>4096-byte values: baseline 9.41 ms. Coalesced 16: 3.08 ms (0.33× time,
///     0.74× allocations). Four writers: 5.10 ms (0.54× time). Both: 2.78 ms (0.30× time,
///     0.74× allocations).</item>
/// </list>
/// <para>Per-message transport overhead dominates small operations: ~7.8 of the 8.5 µs per
/// 128-byte operation disappears when 16 operations share one message. Independent writers help
/// less (2.4× at 128 bytes) and stack with coalescing. These are loopback numbers with an echo
/// server; they bound the cluster-side win from above and say nothing about tail latency under
/// mixed traffic — the cluster validation in the owning spec still gates a wire change.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InterNodeStreamWriteBenchmark
{
    private const int Ops = 1024;

    private const int CoalesceFactor = 16;

    private const int Streams = 4;

    [Params(128, 4096)]
    public int ValueBytes;

    private WebApplication app = null!;

    private GrpcChannel channel = null!;

    private AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse>[] calls = null!;

    private SemaphoreSlim[] writeSemaphores = null!;

    private byte[] value = null!;

    private string[] keys = null!;

    private int requestId;

    private int received;

    private int expected;

    private TaskCompletionSource done = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(static options =>
            options.Listen(IPAddress.Loopback, 0, static listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc();

        app = builder.Build();
        app.MapGrpcService<EchoKeyValuerService>();
        await app.StartAsync();

        string address = app.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses.First();

        channel = GrpcChannel.ForAddress(address);
        KeyValuer.KeyValuerClient client = new(channel);

        calls = new AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse>[Streams];
        writeSemaphores = new SemaphoreSlim[Streams];

        for (int i = 0; i < Streams; i++)
        {
            calls[i] = client.BatchServerKeyValueRequests();
            writeSemaphores[i] = new(1, 1);
            _ = ReadLoop(calls[i]);
        }

        value = new byte[ValueBytes];
        value[0] = 1;

        keys = new string[Ops];
        for (int i = 0; i < Ops; i++)
            keys[i] = $"bench/key/{i}";
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        foreach (AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> call in calls)
            call.Dispose();

        channel.Dispose();
        await app.StopAsync();
    }

    /// <summary>
    /// Counts echoed per-operation results across all streams. The expected count is set before
    /// the first write of an invocation, so the completion signal cannot fire early.
    /// </summary>
    private async Task ReadLoop(AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> call)
    {
        await foreach (GrpcBatchServerKeyValueResponse response in call.ResponseStream.ReadAllAsync())
        {
            int operations = response.Type == GrpcServerBatchType.ServerTrySetManyKeyValue
                ? response.TrySetManyKeyValue.Items.Count
                : 1;

            if (Interlocked.Add(ref received, operations) >= Volatile.Read(ref expected))
                done.TrySetResult();
        }
    }

    private void BeginInvocation()
    {
        done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Interlocked.Exchange(ref received, 0);
        Volatile.Write(ref expected, Ops);
    }

    /// <summary>
    /// One semaphore-serialized write, the same discipline <c>WriteBoundedAsync</c> applies in
    /// production (minus the stall-eviction bookkeeping, which costs nothing on the happy path).
    /// </summary>
    private async Task WriteSerialized(int stream, GrpcBatchServerKeyValueRequest request)
    {
        await writeSemaphores[stream].WaitAsync().ConfigureAwait(false);

        try
        {
            await calls[stream].RequestStream.WriteAsync(request).ConfigureAwait(false);
        }
        finally
        {
            writeSemaphores[stream].Release();
        }
    }

    private GrpcBatchServerKeyValueRequest BuildSingleOpEnvelope(int op)
    {
        return new()
        {
            Type = GrpcServerBatchType.ServerTrySetKeyValue,
            RequestId = Interlocked.Increment(ref requestId),
            TrySetKeyValue = new()
            {
                TransactionIdNode = 1,
                TransactionIdPhysical = 100,
                TransactionIdCounter = (uint)op,
                Key = keys[op],
                Value = UnsafeByteOperations.UnsafeWrap(value),
                CompareRevision = 0,
                Flags = GrpcKeyValueFlags.Set,
                ExpiresMs = 30_000,
                Durability = GrpcKeyValueDurability.Persistent,
                RoutedGeneration = 1
            }
        };
    }

    private GrpcBatchServerKeyValueRequest BuildCoalescedEnvelope(int firstOp, int count)
    {
        GrpcTrySetManyKeyValueRequest many = new();
        many.Items.Capacity = count;

        for (int i = 0; i < count; i++)
        {
            many.Items.Add(new GrpcTrySetManyKeyValueRequestItem
            {
                TransactionIdNode = 1,
                TransactionIdPhysical = 100,
                TransactionIdCounter = (uint)(firstOp + i),
                Key = keys[firstOp + i],
                Value = UnsafeByteOperations.UnsafeWrap(value),
                CompareRevision = 0,
                Flags = GrpcKeyValueFlags.Set,
                ExpiresMs = 30_000,
                Durability = GrpcKeyValueDurability.Persistent,
                RoutedGeneration = 1
            });
        }

        return new()
        {
            Type = GrpcServerBatchType.ServerTrySetManyKeyValue,
            RequestId = Interlocked.Increment(ref requestId),
            TrySetManyKeyValue = many
        };
    }

    [Benchmark(Baseline = true)]
    public async Task OneStreamPerOpWrites()
    {
        BeginInvocation();

        for (int op = 0; op < Ops; op++)
            await WriteSerialized(0, BuildSingleOpEnvelope(op));

        await done.Task;
    }

    [Benchmark]
    public async Task OneStreamCoalesced16()
    {
        BeginInvocation();

        for (int firstOp = 0; firstOp < Ops; firstOp += CoalesceFactor)
            await WriteSerialized(0, BuildCoalescedEnvelope(firstOp, CoalesceFactor));

        await done.Task;
    }

    [Benchmark]
    public async Task FourStreamsPerOpWrites()
    {
        BeginInvocation();

        Task[] writers = new Task[Streams];

        for (int stream = 0; stream < Streams; stream++)
        {
            int owned = stream;
            writers[stream] = Task.Run(async () =>
            {
                for (int op = owned; op < Ops; op += Streams)
                    await WriteSerialized(owned, BuildSingleOpEnvelope(op));
            });
        }

        await Task.WhenAll(writers);
        await done.Task;
    }

    [Benchmark]
    public async Task FourStreamsCoalesced16()
    {
        BeginInvocation();

        Task[] writers = new Task[Streams];
        int opsPerStream = Ops / Streams;

        for (int stream = 0; stream < Streams; stream++)
        {
            int owned = stream;
            writers[stream] = Task.Run(async () =>
            {
                int first = owned * opsPerStream;

                for (int firstOp = first; firstOp < first + opsPerStream; firstOp += CoalesceFactor)
                    await WriteSerialized(owned, BuildCoalescedEnvelope(firstOp, CoalesceFactor));
            });
        }

        await Task.WhenAll(writers);
        await done.Task;
    }

    /// <summary>
    /// Echoes one per-operation result for every request, mirroring the reply shape of the real
    /// server batcher (a set-many request gets one response item per input item) with no
    /// execution behind it.
    /// </summary>
    private sealed class EchoKeyValuerService : KeyValuer.KeyValuerBase
    {
        public override async Task BatchServerKeyValueRequests(
            IAsyncStreamReader<GrpcBatchServerKeyValueRequest> requestStream,
            IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
            ServerCallContext context)
        {
            await foreach (GrpcBatchServerKeyValueRequest request in requestStream.ReadAllAsync(context.CancellationToken))
            {
                GrpcBatchServerKeyValueResponse response = new()
                {
                    Type = request.Type,
                    RequestId = request.RequestId
                };

                if (request.Type == GrpcServerBatchType.ServerTrySetManyKeyValue)
                {
                    GrpcTrySetManyKeyValueResponse many = new();
                    many.Items.Capacity = request.TrySetManyKeyValue.Items.Count;

                    foreach (GrpcTrySetManyKeyValueRequestItem item in request.TrySetManyKeyValue.Items)
                    {
                        many.Items.Add(new GrpcTrySetManyKeyValueResponseItem
                        {
                            Key = item.Key,
                            Type = GrpcKeyValueResponseType.TypeSet,
                            Revision = 1
                        });
                    }

                    response.TrySetManyKeyValue = many;
                }
                else
                {
                    response.TrySetKeyValue = new()
                    {
                        Type = GrpcKeyValueResponseType.TypeSet,
                        Revision = 1
                    };
                }

                await responseStream.WriteAsync(response);
            }
        }
    }
}

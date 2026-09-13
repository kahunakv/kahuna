
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Google.Protobuf;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The peer-certificate gate on Kahuna's node-only gRPC surfaces: the trust matrix itself, and that every
/// guarded entry point refuses before it touches state.
/// </summary>
public sealed class TestNodeTransportGate : IDisposable
{
    private readonly X509Certificate2 trusted = CreateCertificate("trusted", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

    private readonly X509Certificate2 untrusted = CreateCertificate("untrusted", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

    private readonly X509Certificate2 expired = CreateCertificate("expired", DateTimeOffset.UtcNow.AddDays(-30), DateTimeOffset.UtcNow.AddDays(-1));

    public void Dispose()
    {
        trusted.Dispose();
        untrusted.Dispose();
        expired.Dispose();
    }

    public enum Caller
    {
        NoCertificate,
        Expired,
        Untrusted,
        Trusted,
        TrustedOverCleartext,
        NoHttpContext
    }

    [Theory]
    [InlineData(Caller.NoCertificate, RaftTransportAuthenticationStatus.CertificateRequired)]
    [InlineData(Caller.Expired, RaftTransportAuthenticationStatus.CertificateExpired)]
    [InlineData(Caller.Untrusted, RaftTransportAuthenticationStatus.CertificateUntrusted)]
    [InlineData(Caller.TrustedOverCleartext, RaftTransportAuthenticationStatus.TlsRequired)]
    [InlineData(Caller.NoHttpContext, RaftTransportAuthenticationStatus.TlsRequired)]
    public void MutualTlsRefusesAnythingButATrustedPeer(Caller caller, RaftTransportAuthenticationStatus expected)
    {
        NodeTransportGate gate = MutualTlsGate();

        RpcException ex = Assert.Throws<RpcException>(() => gate.RequirePeer(Context(caller)));

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
        Assert.Equal(expected.ToString(), ex.Status.Detail);
    }

    [Fact]
    public void MutualTlsAdmitsATrustedPeer()
    {
        NodeTransportGate gate = MutualTlsGate();

        Assert.True(gate.IsEnforced);
        gate.RequirePeer(Context(Caller.Trusted));
    }

    [Theory]
    [InlineData(RaftNodeAuthenticationMode.Disabled)]
    [InlineData(RaftNodeAuthenticationMode.SharedSecret)]
    public void OtherModesAdmitEveryone(RaftNodeAuthenticationMode mode)
    {
        NodeTransportGate gate = new(
            new RaftTransportAuthenticator(new() { NodeAuthenticationMode = mode, SharedSecret = "secret" }),
            NullLogger<NodeTransportGate>.Instance);

        Assert.False(gate.IsEnforced);
        gate.RequirePeer(Context(Caller.NoHttpContext));
        NodeTransportGate.Disabled.RequirePeer(Context(Caller.NoHttpContext));
    }

    [Theory]
    [InlineData(Caller.NoCertificate)]
    [InlineData(Caller.Expired)]
    [InlineData(Caller.Untrusted)]
    [InlineData(Caller.TrustedOverCleartext)]
    public async Task ForwardedSequenceCallsRefuseUntrustedCallersBeforeServing(Caller caller)
    {
        RecordingKahuna kahuna = new();
        SequencesService service = new(kahuna, MutualTlsGate(), NullLogger<IKahuna>.Instance);

        foreach (Func<ServerCallContext, Task> call in SequenceCalls(service))
        {
            RpcException ex = await Assert.ThrowsAsync<RpcException>(() => call(Context(caller, forwarded: true)));
            Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
        }

        Assert.Empty(kahuna.Calls);
    }

    [Fact]
    public async Task ForwardedSequenceCallsFromATrustedPeerTakeTheForwardedPath()
    {
        RecordingKahuna kahuna = new();
        SequencesService service = new(kahuna, MutualTlsGate(), NullLogger<IKahuna>.Instance);

        foreach (Func<ServerCallContext, Task> call in SequenceCalls(service))
            await call(Context(Caller.Trusted, forwarded: true));

        Assert.Equal(["Create", "Update", "Get", "Next", "Reserve", "Delete"], kahuna.Calls);
    }

    [Fact]
    public async Task ApplicationSequenceCallsNeedNoCertificate()
    {
        RecordingKahuna kahuna = new();
        SequencesService service = new(kahuna, MutualTlsGate(), NullLogger<IKahuna>.Instance);

        foreach (Func<ServerCallContext, Task> call in SequenceCalls(service))
            await call(Context(Caller.NoCertificate, forwarded: false));

        Assert.Equal(["LocateAndCreate", "LocateAndUpdate", "LocateAndGet", "LocateAndNext", "LocateAndReserve", "LocateAndDelete"], kahuna.Calls);
    }

    [Fact]
    public async Task DisabledModeServesForwardedCallsWithoutACertificate()
    {
        RecordingKahuna kahuna = new();
        SequencesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

        foreach (Func<ServerCallContext, Task> call in SequenceCalls(service))
            await call(Context(Caller.NoHttpContext, forwarded: true));

        Assert.Equal(["Create", "Update", "Get", "Next", "Reserve", "Delete"], kahuna.Calls);
    }

    [Fact]
    public async Task KeyValueBatchStreamIsRefusedBeforeItsFirstRead()
    {
        KeyValuesService service = new(new RecordingKahuna(), MutualTlsGate(), NullLogger<IKahuna>.Instance);
        CountingReader<GrpcBatchServerKeyValueRequest> reader = new();

        RpcException ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.BatchServerKeyValueRequests(reader, new NullWriter<GrpcBatchServerKeyValueResponse>(), Context(Caller.Untrusted)));

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
        Assert.Equal(0, reader.MoveNextCalls);
    }

    [Fact]
    public async Task LockBatchStreamIsRefusedBeforeItsFirstRead()
    {
        LocksService service = new(new RecordingKahuna(), new KahunaConfiguration(), null!, MutualTlsGate(), NullLogger<IKahuna>.Instance);
        CountingReader<GrpcBatchServerLockRequest> reader = new();

        RpcException ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.BatchServerLockRequests(reader, new NullWriter<GrpcBatchServerLockResponse>(), Context(Caller.NoCertificate)));

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
        Assert.Equal(0, reader.MoveNextCalls);
    }

    [Fact]
    public async Task BatchStreamsFromATrustedPeerAreServed()
    {
        RecordingKahuna kahuna = new();
        LocksService locks = new(kahuna, new KahunaConfiguration(), null!, MutualTlsGate(), NullLogger<IKahuna>.Instance);
        KeyValuesService keyValues = new(kahuna, MutualTlsGate(), NullLogger<IKahuna>.Instance);
        CountingReader<GrpcBatchServerLockRequest> lockReader = new();
        CountingReader<GrpcBatchServerKeyValueRequest> keyValueReader = new();

        await locks.BatchServerLockRequests(lockReader, new NullWriter<GrpcBatchServerLockResponse>(), Context(Caller.Trusted));
        await keyValues.BatchServerKeyValueRequests(keyValueReader, new NullWriter<GrpcBatchServerKeyValueResponse>(), Context(Caller.Trusted));

        Assert.Equal(1, lockReader.MoveNextCalls);
        Assert.Equal(1, keyValueReader.MoveNextCalls);
    }

    [Fact]
    public async Task UnaryParticipantMethodsAreNodeOnly()
    {
        KeyValuesService service = new(new RecordingKahuna(), MutualTlsGate(), NullLogger<IKahuna>.Instance);

        Func<ServerCallContext, Task>[] calls =
        [
            c => service.TryPrepareMutations(new(), c),
            c => service.TryPrepareManyMutations(new(), c),
            c => service.TryCommitMutations(new(), c),
            c => service.TryCommitManyMutations(new(), c),
            c => service.TryRollbackMutations(new(), c),
            c => service.TryRollbackManyMutations(new(), c)
        ];

        foreach (Func<ServerCallContext, Task> call in calls)
        {
            RpcException ex = await Assert.ThrowsAsync<RpcException>(() => call(Context(Caller.NoCertificate)));
            Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
        }
    }

    [Fact]
    public async Task MappingTheServicesWithoutAGateFailsAtStartup()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Services.AddGrpc();
        await using WebApplication app = builder.Build();

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(app.MapGrpcKahunaRoutes);

        Assert.Contains(nameof(NodeTransportGateServiceCollectionExtensions.AddNodeTransportGate), ex.Message);
    }

    private NodeTransportGate MutualTlsGate() => new(
        new RaftTransportAuthenticator(new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
            TrustedClientCertificateThumbprints = [Convert.ToHexString(SHA256.HashData(trusted.RawData))]
        }),
        NullLogger<NodeTransportGate>.Instance);

    private static IEnumerable<Func<ServerCallContext, Task>> SequenceCalls(SequencesService service) =>
    [
        c => service.CreateSequence(new() { Name = "seq" }, c),
        c => service.UpdateSequence(new() { Name = "seq" }, c),
        c => service.GetSequence(new() { Name = "seq" }, c),
        c => service.NextSequenceValue(new() { Name = "seq" }, c),
        c => service.ReserveSequenceRange(new() { Name = "seq", Count = 1 }, c),
        c => service.DeleteSequence(new() { Name = "seq" }, c)
    ];

    private FakeCallContext Context(Caller caller, bool forwarded = false)
    {
        Metadata headers = forwarded ? new() { { "kahuna-forwarded", "1" } } : new();
        FakeCallContext context = new(headers);

        if (caller == Caller.NoHttpContext)
            return context;

        DefaultHttpContext httpContext = new();
        httpContext.Request.Scheme = caller == Caller.TrustedOverCleartext ? "http" : "https";
        httpContext.Connection.ClientCertificate = caller switch
        {
            Caller.Expired => expired,
            Caller.Untrusted => untrusted,
            Caller.Trusted or Caller.TrustedOverCleartext => trusted,
            _ => null
        };

        // The key gRPC's GetHttpContext() reads.
        context.UserState["__HttpContext"] = httpContext;
        return context;
    }

    private static X509Certificate2 CreateCertificate(string name, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        return new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256).CreateSelfSigned(notBefore, notAfter);
    }

    /// <summary>Records which sequence entry point ran; the forwarded path and the routed path differ.</summary>
    private sealed class RecordingKahuna : FakeKahunaBase
    {
        public List<string> Calls { get; } = [];

        private Task<(SequenceResponseType, long)> Revision(string call)
        {
            Calls.Add(call);
            return Task.FromResult((SequenceResponseType.Success, 1L));
        }

        private Task<(SequenceResponseType, SequenceAllocation)> Allocation(string call)
        {
            Calls.Add(call);
            return Task.FromResult((SequenceResponseType.MustRetry, default(SequenceAllocation)));
        }

        private Task<SequenceResponseType> Deleted(string call)
        {
            Calls.Add(call);
            return Task.FromResult(SequenceResponseType.Success);
        }

        private Task<(SequenceResponseType, ReadOnlySequenceEntry?)> Entry(string call)
        {
            Calls.Add(call);
            return Task.FromResult<(SequenceResponseType, ReadOnlySequenceEntry?)>((SequenceResponseType.NotFound, null));
        }

        public override Task<(SequenceResponseType, long)> CreateSequence(string name, long initialValue, long increment, long? maxValue, int? blockSize, SequenceDurability durability, CancellationToken cancellationToken) => Revision("Create");
        public override Task<(SequenceResponseType, long)> LocateAndCreateSequence(string name, long initialValue, long increment, long? maxValue, int? blockSize, SequenceDurability durability, CancellationToken cancellationToken) => Revision("LocateAndCreate");
        public override Task<(SequenceResponseType, long)> UpdateSequence(string name, SequenceUpdate update, SequenceDurability durability, CancellationToken cancellationToken) => Revision("Update");
        public override Task<(SequenceResponseType, long)> LocateAndUpdateSequence(string name, SequenceUpdate update, SequenceDurability durability, CancellationToken cancellationToken) => Revision("LocateAndUpdate");
        public override Task<(SequenceResponseType, ReadOnlySequenceEntry?)> GetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => Entry("Get");
        public override Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => Entry("LocateAndGet");
        public override Task<(SequenceResponseType, SequenceAllocation)> NextSequenceValue(string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => Allocation("Next");
        public override Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => Allocation("LocateAndNext");
        public override Task<(SequenceResponseType, SequenceAllocation)> ReserveSequenceRange(string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => Allocation("Reserve");
        public override Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => Allocation("LocateAndReserve");
        public override Task<SequenceResponseType> DeleteSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => Deleted("Delete");
        public override Task<SequenceResponseType> LocateAndDeleteSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => Deleted("LocateAndDelete");
    }

    private sealed class CountingReader<T> : IAsyncStreamReader<T>
    {
        public int MoveNextCalls { get; private set; }

        public T Current => throw new InvalidOperationException("The stream is empty.");

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            MoveNextCalls++;
            return Task.FromResult(false);
        }
    }

    private sealed class NullWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    private sealed class FakeCallContext(Metadata headers) : ServerCallContext
    {
        protected override string MethodCore => "/test/Method";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "ipv4:127.0.0.1:5000";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => headers;
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore { get; } = [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(null, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}

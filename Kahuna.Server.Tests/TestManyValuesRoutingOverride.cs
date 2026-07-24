using Grpc.Core;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The client-facing batch-read gRPC endpoints must route each key to its partition leader, exactly like the
/// single-key <c>TryGetKeyValue</c> endpoint. A client sends the whole batch to one node as a unary RPC without
/// per-key routing, so if the endpoint reads this node's local actors directly it returns stale or absent state
/// for any key whose partition this node does not lead — dropping a committed value a routed read (or a
/// same-transaction scan) sees. These tests pin the override to the routing method; the previous implementation
/// called the non-locating local read and would fail them.
/// </summary>
public sealed class TestManyValuesRoutingOverride
{
    private static readonly HLCTimestamp Ts = new(1, 5000, 0);

    private static ServerCallContext Context() => new StubServerCallContext();

    [Fact]
    public async Task GetManyOverride_RoutesPerKey_NotLocalActorRead()
    {
        RoutingRecordingKahuna fake = new();
        KeyValuesService service = new(fake, NullLogger<IKahuna>.Instance);

        GrpcTryGetManyValuesRequest request = new()
        {
            ReadTimestampNode = Ts.N, ReadTimestampPhysical = Ts.L, ReadTimestampCounter = Ts.C
        };
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "a", Revision = -1 });
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "b", Revision = -1 });

        GrpcTryGetManyValuesResponse response = await service.TryGetManyValues(request, Context());

        // The routing method was used, and the non-locating local read was not.
        Assert.True(fake.LocateGetCalled, "override must call the locating LocateAndTryGetManyValues");
        Assert.False(fake.LocalGetCalled, "override must not call the non-locating TryGetManyValues");
        Assert.Equal(Ts, fake.LocateGetReadTimestamp);

        // And it faithfully returns every routed key's result — the whole point of the contract.
        Assert.Equal(2, response.Items.Count);
        Assert.All(response.Items, item => Assert.Equal(GrpcKeyValueResponseType.TypeGot, item.Type));
    }

    [Fact]
    public async Task ExistsManyOverride_RoutesPerKey_NotLocalActorRead()
    {
        RoutingRecordingKahuna fake = new();
        KeyValuesService service = new(fake, NullLogger<IKahuna>.Instance);

        GrpcTryExistsManyValuesRequest request = new()
        {
            ReadTimestampNode = Ts.N, ReadTimestampPhysical = Ts.L, ReadTimestampCounter = Ts.C
        };
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "a", Revision = -1 });

        GrpcTryExistsManyValuesResponse response = await service.TryExistsManyValues(request, Context());

        Assert.True(fake.LocateExistsCalled, "override must call the locating LocateAndTryExistsManyValues");
        Assert.False(fake.LocalExistsCalled, "override must not call the non-locating TryExistsManyValues");
        Assert.Single(response.Items);
    }

    /// <summary>
    /// Records which batch-read manager method the service invoked. The locating methods return a Get/Exists per
    /// requested key (so the response is well-formed); the non-locating ones flip a flag and throw, so any call to
    /// them fails the test loudly rather than silently satisfying it.
    /// </summary>
    private sealed class RoutingRecordingKahuna : FakeKahunaBase
    {
        public bool LocateGetCalled { get; private set; }
        public bool LocalGetCalled { get; private set; }
        public bool LocateExistsCalled { get; private set; }
        public bool LocalExistsCalled { get; private set; }
        public HLCTimestamp LocateGetReadTimestamp { get; private set; }

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
            HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys,
            CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            LocateGetCalled = true;
            LocateGetReadTimestamp = readTimestamp;
            return Task.FromResult(BuildGot(keys));
        }

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
            HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys,
            CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            LocateExistsCalled = true;
            return Task.FromResult(BuildGot(keys, exists: true));
        }

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(
            HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys)
        {
            LocalGetCalled = true;
            throw new InvalidOperationException("non-locating TryGetManyValues must not be called by the client-facing override");
        }

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(
            HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys)
        {
            LocalExistsCalled = true;
            throw new InvalidOperationException("non-locating TryExistsManyValues must not be called by the client-facing override");
        }

        private static List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> BuildGot(
            List<(string key, long revision, KeyValueDurability durability)> keys, bool exists = false)
        {
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> result = new(keys.Count);
            foreach ((string key, _, KeyValueDurability durability) in keys)
            {
                ReadOnlyKeyValueEntry entry = new(
                    "v"u8.ToArray(), 1, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);
                result.Add((exists ? KeyValueResponseType.Exists : KeyValueResponseType.Get, key, durability, entry));
            }
            return result;
        }
    }

    /// <summary>Minimal <see cref="ServerCallContext"/> for the handler under test: it reads only the cancellation
    /// token. Every other member is unused here and throws if touched.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override System.DateTime DeadlineCore => System.DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}

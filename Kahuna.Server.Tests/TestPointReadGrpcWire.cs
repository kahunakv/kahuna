using Google.Protobuf;
using Grpc.Core;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// A point read served over gRPC must carry the entry's full metadata — LastModified, LastUsed and State —
/// not just the value and revision. LastModified is the field callers round-trip into a later snapshot read,
/// and <c>HLCTimestamp.Zero</c> doubles as the "read latest" sentinel, so a response that omits it silently
/// degrades every snapshot read built on it into a latest read. The inter-node hop and the client-facing
/// gRPC endpoint share this handler, so the omission affected both. These tests drive the real gRPC entry
/// points and round-trip the reply through protobuf so protobuf defaults cannot masquerade as real values.
/// </summary>
public sealed class TestPointReadGrpcWire
{
    private static readonly HLCTimestamp Expires = new(3, 9000, 2);
    private static readonly HLCTimestamp LastUsed = new(2, 8000, 5);
    private static readonly HLCTimestamp LastModified = new(1, 7000, 4);

    private static readonly byte[] Value = [10, 20, 30];

    private static ReadOnlyKeyValueEntry Entry(byte[]? value) =>
        new(value, 7, Expires, LastUsed, LastModified, KeyValueState.Set);

    private static ServerCallContext Context() => new StubServerCallContext();

    [Fact]
    public async Task TryGetKeyValue_CarriesFullEntryMetadataOverTheWire()
    {
        KeyValuesService service = new(
            new FixedPointReadKahuna(KeyValueResponseType.Get, Entry(Value)), NullLogger<IKahuna>.Instance);

        GrpcTryGetKeyValueResponse response = await service.TryGetKeyValue(
            new GrpcTryGetKeyValueRequest { Key = "greeting", Revision = -1 }, Context());

        GrpcTryGetKeyValueResponse onWire = GrpcTryGetKeyValueResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal(GrpcKeyValueResponseType.TypeGot, onWire.Type);
        Assert.Equal(7, onWire.Revision);
        Assert.Equal(Value, onWire.Value.ToByteArray());

        Assert.Equal(Expires, new HLCTimestamp(onWire.ExpiresNode, onWire.ExpiresPhysical, onWire.ExpiresCounter));
        Assert.Equal(LastUsed, new HLCTimestamp(onWire.LastUsedNode, onWire.LastUsedPhysical, onWire.LastUsedCounter));
        Assert.Equal(LastModified, new HLCTimestamp(onWire.LastModifiedNode, onWire.LastModifiedPhysical, onWire.LastModifiedCounter));
        Assert.Equal(GrpcKeyValueState.StateSet, onWire.State);
    }

    [Fact]
    public async Task TryExistsKeyValue_CarriesFullEntryMetadataOverTheWire()
    {
        KeyValuesService service = new(
            new FixedPointReadKahuna(KeyValueResponseType.Exists, Entry(null)), NullLogger<IKahuna>.Instance);

        GrpcTryExistsKeyValueResponse response = await service.TryExistsKeyValue(
            new GrpcTryExistsKeyValueRequest { Key = "greeting", Revision = -1 }, Context());

        GrpcTryExistsKeyValueResponse onWire = GrpcTryExistsKeyValueResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal(GrpcKeyValueResponseType.TypeExists, onWire.Type);
        Assert.Equal(7, onWire.Revision);

        Assert.Equal(Expires, new HLCTimestamp(onWire.ExpiresNode, onWire.ExpiresPhysical, onWire.ExpiresCounter));
        Assert.Equal(LastUsed, new HLCTimestamp(onWire.LastUsedNode, onWire.LastUsedPhysical, onWire.LastUsedCounter));
        Assert.Equal(LastModified, new HLCTimestamp(onWire.LastModifiedNode, onWire.LastModifiedPhysical, onWire.LastModifiedCounter));
        Assert.Equal(GrpcKeyValueState.StateSet, onWire.State);
    }

    /// <summary>Answers the routed point-read entry points with one fixed entry.</summary>
    private sealed class FixedPointReadKahuna : FakeKahunaBase
    {
        private readonly (KeyValueResponseType, ReadOnlyKeyValueEntry?) result;

        public FixedPointReadKahuna(KeyValueResponseType type, ReadOnlyKeyValueEntry? entry)
        {
            result = (type, entry);
        }

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult(result);

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult(result);
    }

    /// <summary>Minimal context: the service reads only the cancellation token.</summary>
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

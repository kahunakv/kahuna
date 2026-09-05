/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Client.Communication;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// A key that holds no value is not a key that holds zero bytes, and every layer here must keep the two
/// apart. The store keeps them apart in memory, the REST transport carries the first as an explicit null,
/// and the proto3 <c>optional</c> fields exist to carry it as an absent field. What broke the agreement was
/// a conditional that still ran the generated setter — <c>Value = v is not null ? Wrap(v) : null</c> — so
/// the gRPC client threw <c>ArgumentNullException</c> on a null payload before the request left the process,
/// while REST accepted the same call. The decoders on the other side had the mirror-image fault: they read
/// the field without its presence flag, which promotes an absent value to an empty one.
///
/// These tests pin the whole contract in both directions:
///   • the client encodes a null payload as an absent field and an empty payload as a present, empty one;
///   • the gRPC service decodes those back to null and empty respectively, after a real protobuf round trip;
///   • the committed Raft log record survives the same round trip, so a follower applies what the leader
///     holds rather than an empty array in its place;
///   • a read of a value-less key answers with an absent field, which is what makes the client return null.
/// </summary>
public sealed class TestKeyValuePayloadPresence
{
    private static ServerCallContext Context() => new StubServerCallContext();

    /// <summary>Serializes and re-parses a message so a field set in memory cannot masquerade as one on the wire.</summary>
    private static GrpcTrySetManyKeyValueRequest OnWire(GrpcTrySetManyKeyValueRequest request) =>
        GrpcTrySetManyKeyValueRequest.Parser.ParseFrom(request.ToByteArray());

    [Fact]
    public void SetMany_EncodesANullValueAsAnAbsentFieldAndAnEmptyValueAsAPresentOne()
    {
        List<KahunaSetKeyValueRequestItem> source =
        [
            new() { Key = "bytes", Value = [1, 2], ExpiresMs = 10, Durability = KeyValueDurability.Ephemeral },
            new() { Key = "null-value", Value = null, Durability = KeyValueDurability.Persistent },
            new() { Key = "empty-value", Value = [] },
            new() { Key = null, Value = [7] }
        ];

        RepeatedField<GrpcTrySetManyKeyValueRequestItem> target = [];

        // Before the fix this call threw ArgumentNullException on the second item and never reached the wire.
        GrpcCommunication.AddSetManyKeyValueRequestItems(target, source);

        Assert.Equal(4, target.Count);
        Assert.Equal(4, target.Capacity);

        Assert.True(target[0].HasValue);
        Assert.Equal([1, 2], target[0].Value.ToByteArray());
        Assert.Equal(10, target[0].ExpiresMs);
        Assert.Equal((int)KeyValueDurability.Ephemeral, (int)target[0].Durability);

        // The distinction the whole fix is about: absent for a null payload, present-and-empty for no bytes.
        Assert.False(target[1].HasValue);
        Assert.Equal((int)KeyValueDurability.Persistent, (int)target[1].Durability);

        Assert.True(target[2].HasValue);
        Assert.Empty(target[2].Value.ToByteArray());

        // A null key is a caller error the server reports as InvalidInput, not a transport-level throw.
        Assert.Equal("", target[3].Key);
    }

    [Fact]
    public void DeleteMany_EncodesANullKeyAsAnEmptyKeyInsteadOfThrowing()
    {
        RepeatedField<GrpcTryDeleteManyKeyValueRequestItem> target = [];

        GrpcCommunication.AddDeleteManyKeyValueRequestItems(target,
            [new KahunaDeleteKeyValueRequestItem { Key = null, Durability = KeyValueDurability.Persistent }]);

        Assert.Single(target);
        Assert.Equal("", target[0].Key);
    }

    [Fact]
    public async Task SetMany_AbsentValueReachesTheStoreAsNullAndAnEmptyValueAsEmpty()
    {
        RepeatedField<GrpcTrySetManyKeyValueRequestItem> encoded = [];
        GrpcCommunication.AddSetManyKeyValueRequestItems(encoded,
        [
            new KahunaSetKeyValueRequestItem { Key = "null-value", Value = null },
            new KahunaSetKeyValueRequestItem { Key = "empty-value", Value = [] },
            new KahunaSetKeyValueRequestItem { Key = "bytes", Value = [9] }
        ]);

        GrpcTrySetManyKeyValueRequest request = new();
        request.Items.AddRange(encoded);

        CapturingSetManyKahuna kahuna = new();
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);

        await service.TrySetManyKeyValue(OnWire(request), Context());

        Assert.NotNull(kahuna.Captured);
        Assert.Equal(3, kahuna.Captured!.Count);

        Assert.Null(kahuna.Captured[0].Value);

        Assert.NotNull(kahuna.Captured[1].Value);
        Assert.Empty(kahuna.Captured[1].Value!);

        Assert.Equal([9], kahuna.Captured[2].Value);
    }

    [Fact]
    public async Task Set_AbsentValueAndCompareValueReachTheStoreAsNull()
    {
        // Neither field is assigned, which is exactly what the fixed client encoder leaves on the wire for
        // a null payload.
        GrpcTrySetKeyValueRequest request = new() { Key = "k", Flags = GrpcKeyValueFlags.SetIfEqualToValue };

        CapturingSetKahuna kahuna = new();
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);

        await service.TrySetKeyValue(
            GrpcTrySetKeyValueRequest.Parser.ParseFrom(request.ToByteArray()), Context());

        Assert.True(kahuna.Called);
        Assert.Null(kahuna.Value);
        Assert.Null(kahuna.CompareValue);

        GrpcTrySetKeyValueRequest withEmptyValue = new()
        {
            Key = "k",
            Value = ByteString.Empty,
            CompareValue = ByteString.Empty
        };

        CapturingSetKahuna emptyKahuna = new();
        KeyValuesService emptyService = new(emptyKahuna, NullLogger<IKahuna>.Instance);

        await emptyService.TrySetKeyValue(
            GrpcTrySetKeyValueRequest.Parser.ParseFrom(withEmptyValue.ToByteArray()), Context());

        Assert.NotNull(emptyKahuna.Value);
        Assert.Empty(emptyKahuna.Value!);
        Assert.NotNull(emptyKahuna.CompareValue);
        Assert.Empty(emptyKahuna.CompareValue!);
    }

    [Fact]
    public void CommittedLogRecord_KeepsAValuelessSetApartFromAnEmptyOne()
    {
        // A follower applies this record. If the decode promoted the absent value to an empty array, the
        // follower's entry would differ from the leader's for the rest of the key's life.
        Assert.Null(DecodeProposal(null));

        byte[]? empty = DecodeProposal([]);
        Assert.NotNull(empty);
        Assert.Empty(empty!);

        Assert.Equal([4, 5], DecodeProposal([4, 5]));
    }

    private static byte[]? DecodeProposal(byte[]? value)
    {
        HLCTimestamp now = new(1, 500, 0);

        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet, "k", value, revision: 3, noRevision: false,
            expires: HLCTimestamp.Zero, lastUsed: now, lastModified: now,
            KeyValueState.Set, KeyValueDurability.Persistent);

        byte[] record = BaseHandler.SerializeProposal(KeyValueRequestType.TrySet, proposal, now);

        KeyValueMessage message = ReplicationSerializer.UnserializeKeyValueMessage(record);

        (KeyValueState state, byte[]? decoded) = KeyValueMessageDecoder.Decode(message);

        Assert.Equal(KeyValueState.Set, state);

        return decoded;
    }

    [Fact]
    public async Task PointRead_AnswersAValuelessEntryWithAnAbsentField()
    {
        HLCTimestamp stamp = new(1, 700, 0);

        GrpcTryGetKeyValueResponse valueless = await ReadEntry(
            new ReadOnlyKeyValueEntry(null, 7, stamp, stamp, stamp, KeyValueState.Set));

        // An absent field is what makes the client hand the caller null instead of an empty array, which is
        // what the REST transport already returns for the same key.
        Assert.False(valueless.HasValue);

        GrpcTryGetKeyValueResponse empty = await ReadEntry(
            new ReadOnlyKeyValueEntry([], 7, stamp, stamp, stamp, KeyValueState.Set));

        Assert.True(empty.HasValue);
        Assert.Empty(empty.Value.ToByteArray());
    }

    private static async Task<GrpcTryGetKeyValueResponse> ReadEntry(ReadOnlyKeyValueEntry entry)
    {
        KeyValuesService service = new(new FixedGetKahuna(entry), NullLogger<IKahuna>.Instance);

        GrpcTryGetKeyValueResponse response = await service.TryGetKeyValue(
            new GrpcTryGetKeyValueRequest { Key = "k", Revision = -1 }, Context());

        return GrpcTryGetKeyValueResponse.Parser.ParseFrom(response.ToByteArray());
    }

    /// <summary>Records the decoded set-many items instead of writing them.</summary>
    private sealed class CapturingSetManyKahuna : FakeKahunaBase
    {
        public List<KahunaSetKeyValueRequestItem>? Captured { get; private set; }

        public override Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
            List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            Captured = setManyItems;

            List<KahunaSetKeyValueResponseItem> responses = new(setManyItems.Count);
            foreach (KahunaSetKeyValueRequestItem item in setManyItems)
                responses.Add(new KahunaSetKeyValueResponseItem { Key = item.Key, Type = KeyValueResponseType.Set });

            return Task.FromResult(responses);
        }
    }

    /// <summary>Records the decoded single-key payloads instead of writing them.</summary>
    private sealed class CapturingSetKahuna : FakeKahunaBase
    {
        public bool Called { get; private set; }

        public byte[]? Value { get; private set; }

        public byte[]? CompareValue { get; private set; }

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            Called = true;
            Value = value;
            CompareValue = compareValue;

            return Task.FromResult((KeyValueResponseType.Set, 1L, HLCTimestamp.Zero));
        }
    }

    /// <summary>Answers the routed point read with one fixed entry.</summary>
    private sealed class FixedGetKahuna : FakeKahunaBase
    {
        private readonly ReadOnlyKeyValueEntry entry;

        public FixedGetKahuna(ReadOnlyKeyValueEntry entry) => this.entry = entry;

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.Get, entry));
    }

    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override string MethodCore => "";
        protected override string HostCore => "";
        protected override string PeerCore => "";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => [];
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore => [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new("", new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}

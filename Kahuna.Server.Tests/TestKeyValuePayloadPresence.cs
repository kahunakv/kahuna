/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
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
using Kahuna.Server.Communication.Internode;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
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
/// The JSON transport had the same fault on the write side, from a different cause. The source generator
/// writes a byte array through <c>WriteBase64String</c>, which takes a span, and a null array converts to
/// an empty span there. So the generated fast path emitted <c>""</c> for an absent payload, and every REST
/// write flattened the distinction the gRPC transport keeps.
///
/// These tests pin the whole contract in both directions:
///   • the client encodes a null payload as an absent field and an empty payload as a present, empty one;
///   • the gRPC service decodes those back to null and empty respectively, after a real protobuf round trip;
///   • the committed Raft log record survives the same round trip, so a follower applies what the leader
///     holds rather than an empty array in its place;
///   • a read of a value-less key answers with an absent field, which is what makes the client return null;
///   • a routed batch read decodes each item exactly as a point read does, and borrows the parsed bytes
///     instead of copying them a second time;
///   • the client's own batch, scan and transaction decoders keep the same distinction the point read keeps;
///   • the JSON wire writes an absent payload as null and an empty one as an empty string, in requests and
///     in responses, and every payload property on that wire carries the converter that guarantees it.
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

    // ── Batch and scan reads ────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The inter-node batch read is the leg that lagged behind the point read: it read the value field without
    /// its presence flag, so a key holding zero bytes answered null as soon as the read crossed a node boundary,
    /// while the same key answered an empty array when its partition happened to be led locally.
    /// </summary>
    [Fact]
    public async Task BatchRead_KeepsAValuelessEntryApartFromAnEmptyOneAcrossTheInterNodeWire()
    {
        HLCTimestamp stamp = new(1, 900, 0);

        GrpcTryGetManyValuesResponse onWire = await BatchRead(
        [
            (KeyValueResponseType.Get, "null-value", new ReadOnlyKeyValueEntry(null, 11, stamp, stamp, stamp, KeyValueState.Set)),
            (KeyValueResponseType.Get, "empty-value", new ReadOnlyKeyValueEntry([], 12, stamp, stamp, stamp, KeyValueState.Set)),
            (KeyValueResponseType.Get, "bytes", new ReadOnlyKeyValueEntry([1, 2, 3], 13, stamp, stamp, stamp, KeyValueState.Set)),
            (KeyValueResponseType.MustRetry, "unresolved", null)
        ]);

        Assert.Equal(4, onWire.Items.Count);

        // What the sender put on the wire: absent for the value-less key, present for the other two.
        Assert.False(onWire.Items[0].HasValue);
        Assert.True(onWire.Items[1].HasValue);
        Assert.True(onWire.Items[2].HasValue);

        ReadOnlyKeyValueEntry? valueless = GrpcInterNodeCommunication.GetReadOnlyKeyValueEntry(onWire.Items[0]);
        ReadOnlyKeyValueEntry? empty = GrpcInterNodeCommunication.GetReadOnlyKeyValueEntry(onWire.Items[1]);
        ReadOnlyKeyValueEntry? bytes = GrpcInterNodeCommunication.GetReadOnlyKeyValueEntry(onWire.Items[2]);

        Assert.NotNull(valueless);
        Assert.Null(valueless!.Value);
        Assert.Equal(11, valueless.Revision);
        Assert.Equal(stamp, valueless.LastModified);
        Assert.Equal(KeyValueState.Set, valueless.State);

        Assert.NotNull(empty);
        Assert.NotNull(empty!.Value);
        Assert.Empty(empty.Value!);
        Assert.Equal(12, empty.Revision);

        Assert.NotNull(bytes);
        Assert.Equal([1, 2, 3], bytes!.Value);
        Assert.Equal(13, bytes.Revision);

        // A key the leader could not answer carries no entry at all, whatever its value field says.
        Assert.Null(GrpcInterNodeCommunication.GetReadOnlyKeyValueEntry(onWire.Items[3]));

        // The payload is borrowed from the parsed message. A second array here is the allocation the decode
        // used to make for every non-empty value in every batch.
        Assert.True(MemoryMarshal.TryGetArray(onWire.Items[2].Value.Memory, out ArraySegment<byte> parsed));
        Assert.Same(parsed.Array, bytes.Value);
    }

    /// <summary>
    /// Runs the production encoder for a non-locating batch read, puts its response on the wire, and hands the
    /// parsed message back for the inter-node decoder to read.
    /// </summary>
    private static async Task<GrpcTryGetManyValuesResponse> BatchRead(
        List<(KeyValueResponseType type, string key, ReadOnlyKeyValueEntry? entry)> results)
    {
        GrpcTryGetManyValuesRequest request = new();

        foreach ((KeyValueResponseType _, string key, ReadOnlyKeyValueEntry? _) in results)
            request.Items.Add(new GrpcTryManyValuesRequestItem { Key = key, Revision = -1 });

        KeyValuesService service = new(new FixedBatchGetKahuna(results), NullLogger<IKahuna>.Instance);

        GrpcTryGetManyValuesResponse response = await service.TryGetManyValuesInternal(
            GrpcTryGetManyValuesRequest.Parser.ParseFrom(request.ToByteArray()), Context());

        return GrpcTryGetManyValuesResponse.Parser.ParseFrom(response.ToByteArray());
    }

    /// <summary>
    /// The borrow the decoders rely on, and the copy that keeps it honest. A ByteString that owns its whole
    /// backing array hands that array over as it is; one that views a slice of a larger buffer must copy, or a
    /// caller would read the bytes around the payload.
    /// </summary>
    [Fact]
    public void PayloadDecoder_BorrowsAWholeBackingArrayAndCopiesASlice()
    {
        byte[] whole = [1, 2, 3];
        Assert.Same(whole, ByteStringPayload.GetArrayOrNull(true, UnsafeByteOperations.UnsafeWrap(whole)));

        byte[] backing = [9, 1, 2, 9];
        byte[]? slice = ByteStringPayload.GetArrayOrNull(
            true, UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(backing, 1, 2)));

        Assert.NotSame(backing, slice);
        Assert.Equal([1, 2], slice);

        // An absent field stays absent whatever bytes the generated getter substitutes for it.
        Assert.Null(ByteStringPayload.GetArrayOrNull(false, ByteString.Empty));
    }

    /// <summary>
    /// The client's own gRPC decoders read the same presence-tracked fields. A batch read already carried the
    /// distinction; the scan and the script-transaction reads did not, and each answered an empty array for a
    /// key that holds no value — the opposite of what the REST transport returns for the same key.
    /// </summary>
    [Fact]
    public void ClientGrpcReads_KeepAValuelessItemApartFromAnEmptyOne()
    {
        GrpcTryGetManyValuesResponse batch = new();
        batch.Items.Add(new GrpcTryGetManyValuesResponseItem { Key = "null-value" });
        batch.Items.Add(new GrpcTryGetManyValuesResponseItem { Key = "empty-value", Value = ByteString.Empty });
        batch.Items.Add(new GrpcTryGetManyValuesResponseItem { Key = "bytes", Value = ByteString.CopyFrom(1, 2) });

        List<KahunaGetManyKeyValuesResponseItem> read = GrpcCommunication.GetGetManyKeyValuesResponseItems(
            GrpcTryGetManyValuesResponse.Parser.ParseFrom(batch.ToByteArray()).Items);

        AssertPayloadTriple(read[0].Value, read[1].Value, read[2].Value);

        GrpcGetByBucketResponse scan = new();
        scan.Items.Add(new GrpcKeyValueByPrefixItemResponse { Key = "null-value" });
        scan.Items.Add(new GrpcKeyValueByPrefixItemResponse { Key = "empty-value", Value = ByteString.Empty });
        scan.Items.Add(new GrpcKeyValueByPrefixItemResponse { Key = "bytes", Value = ByteString.CopyFrom(1, 2) });

        List<KeyValueGetByBucketItem> scanned = GrpcCommunication.GetByPrefixResponseItems(
            GrpcGetByBucketResponse.Parser.ParseFrom(scan.ToByteArray()).Items);

        AssertPayloadTriple(scanned[0].Value, scanned[1].Value, scanned[2].Value);

        GrpcTryExecuteTransactionScriptResponse script = new();
        script.Values.Add(new GrpcTryExecuteTransactionResponseValue { Key = "null-value" });
        script.Values.Add(new GrpcTryExecuteTransactionResponseValue { Key = "empty-value", Value = ByteString.Empty });
        script.Values.Add(new GrpcTryExecuteTransactionResponseValue { Key = "bytes", Value = ByteString.CopyFrom(1, 2) });

        List<Kahuna.Client.KahunaKeyValueTransactionResultValue> values = GrpcCommunication.GetTransactionValues(
            GrpcTryExecuteTransactionScriptResponse.Parser.ParseFrom(script.ToByteArray()).Values);

        AssertPayloadTriple(values[0].Value, values[1].Value, values[2].Value);
    }

    private static void AssertPayloadTriple(byte[]? valueless, byte[]? empty, byte[]? bytes)
    {
        Assert.Null(valueless);

        Assert.NotNull(empty);
        Assert.Empty(empty!);

        Assert.Equal([1, 2], bytes);
    }

    // ── The JSON wire ───────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SetRequest_WritesAnAbsentPayloadAsNullAndAnEmptyPayloadAsAnEmptyString()
    {
        string absent = JsonSerializer.Serialize(
            new KahunaSetKeyValueRequest { Key = "k" },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        Assert.Contains("\"value\":null", absent);
        Assert.Contains("\"compareValue\":null", absent);

        string empty = JsonSerializer.Serialize(
            new KahunaSetKeyValueRequest { Key = "k", Value = [], CompareValue = [] },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        Assert.Contains("\"value\":\"\"", empty);
        Assert.Contains("\"compareValue\":\"\"", empty);

        // The two bodies carry different writes. The generated fast path made them identical, which is
        // what left a REST caller unable to set a key to no value.
        Assert.NotEqual(absent, empty);

        string bytes = JsonSerializer.Serialize(
            new KahunaSetKeyValueRequest { Key = "k", Value = [1, 2, 3] },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        Assert.Contains("\"value\":\"AQID\"", bytes);
    }

    [Fact]
    public void SetManyRequest_WritesEachItemPayloadWithTheSameDistinction()
    {
        string body = JsonSerializer.Serialize(
            new KahunaSetManyKeyValueRequest
            {
                Items =
                [
                    new KahunaSetKeyValueRequestItem { Key = "null-value", Value = null },
                    new KahunaSetKeyValueRequestItem { Key = "empty-value", Value = [] },
                    new KahunaSetKeyValueRequestItem { Key = "bytes", Value = [9] }
                ]
            },
            KahunaJsonContext.Default.KahunaSetManyKeyValueRequest);

        Assert.Contains("\"value\":null", body);
        Assert.Contains("\"value\":\"\"", body);
        Assert.Contains("\"value\":\"CQ==\"", body);
    }

    [Fact]
    public void SetRequest_DecodesNullAsAnAbsentPayloadAndAnEmptyStringAsZeroBytes()
    {
        // A body the server binds. An omitted field means the same as an explicit null, which is how the
        // gRPC service reads an unset optional field.
        AssertSetRequestDecodesTo("""{"key":"k","value":null,"compareValue":null}""", expectAbsent: true);
        AssertSetRequestDecodesTo("""{"key":"k"}""", expectAbsent: true);
        AssertSetRequestDecodesTo("""{"key":"k","value":"","compareValue":""}""", expectAbsent: false);
    }

    /// <summary>
    /// Checks both decoders that see this body: the server binds it with the reflection-based serializer,
    /// and a client that re-reads a request uses the generated context. The two must agree.
    /// </summary>
    private static void AssertSetRequestDecodesTo(string body, bool expectAbsent)
    {
        KahunaSetKeyValueRequest?[] decoded =
        [
            JsonSerializer.Deserialize(body, KahunaJsonContext.Default.KahunaSetKeyValueRequest),
            JsonSerializer.Deserialize<KahunaSetKeyValueRequest>(body, JsonSerializerOptions.Web)
        ];

        foreach (KahunaSetKeyValueRequest? request in decoded)
        {
            Assert.NotNull(request);

            if (expectAbsent)
            {
                Assert.Null(request!.Value);
                Assert.Null(request.CompareValue);
                continue;
            }

            Assert.NotNull(request!.Value);
            Assert.Empty(request.Value!);
            Assert.NotNull(request.CompareValue);
            Assert.Empty(request.CompareValue!);
        }
    }

    [Fact]
    public void ReadResponses_KeepAValuelessEntryApartFromAnEmptyOneAcrossAJsonRoundTrip()
    {
        // The server writes these with the reflection-based serializer the host configures, and the client
        // reads them back the same way. A key holding no value must not arrive as zero bytes.
        AssertPayloadSurvivesARoundTrip(
            new KahunaGetKeyValueResponse { Value = null },
            new KahunaGetKeyValueResponse { Value = [] },
            static r => r.Value);

        AssertPayloadSurvivesARoundTrip(
            new KahunaGetManyKeyValuesResponseItem { Key = "k", Value = null },
            new KahunaGetManyKeyValuesResponseItem { Key = "k", Value = [] },
            static r => r.Value);

        AssertPayloadSurvivesARoundTrip(
            new KeyValueGetByBucketItem { Key = "k", Value = null },
            new KeyValueGetByBucketItem { Key = "k", Value = [] },
            static r => r.Value);

        AssertPayloadSurvivesARoundTrip(
            new KeyValueTransactionResponse { Value = null },
            new KeyValueTransactionResponse { Value = [] },
            static r => r.Value);

        AssertPayloadSurvivesARoundTrip(
            new KahunaTxKeyValueResponse { Value = null },
            new KahunaTxKeyValueResponse { Value = [] },
            static r => r.Value);

        AssertPayloadSurvivesARoundTrip(
            new KahunaTxKeyValueResponseItem { Key = "k", Value = null },
            new KahunaTxKeyValueResponseItem { Key = "k", Value = [] },
            static r => r.Value);
    }

    private static void AssertPayloadSurvivesARoundTrip<T>(T withNoValue, T withZeroBytes, Func<T, byte[]?> payload)
    {
        T? absent = JsonSerializer.Deserialize<T>(
            JsonSerializer.Serialize(withNoValue, JsonSerializerOptions.Web), JsonSerializerOptions.Web);

        Assert.NotNull(absent);
        Assert.Null(payload(absent!));

        T? empty = JsonSerializer.Deserialize<T>(
            JsonSerializer.Serialize(withZeroBytes, JsonSerializerOptions.Web), JsonSerializerOptions.Web);

        Assert.NotNull(empty);
        Assert.NotNull(payload(empty!));
        Assert.Empty(payload(empty!)!);
    }

    /// <summary>
    /// A structural guard. A payload property added to the JSON wire without the converter would flatten a
    /// null payload to an empty one again, silently and only on that one field, so this fails the moment
    /// such a property appears.
    /// </summary>
    [Fact]
    public void EveryPayloadOnTheJsonWire_CarriesTheConverterThatPreservesAnAbsentValue()
    {
        // These byte arrays are not payloads. A lock owner has no presence flag on either transport, and
        // both coerce a null owner to an empty one, so writing null there would fail server validation
        // instead. A script is a required input rather than stored bytes: null and empty both mean that
        // the caller sent no script, and the server answers InvalidInput either way.
        HashSet<string> notPayloads =
        [
            "KahunaLockRequest.Owner",
            "KahunaGetLockResponse.Owner",
            "KeyValueTransactionRequest.Script",
            "KahunaTxKeyValueRequest.Script"
        ];

        List<string> unprotected = [];

        foreach (Type type in typeof(KahunaSetKeyValueRequest).Assembly.GetTypes())
        {
            if (!type.IsClass)
                continue;

            foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (property.PropertyType != typeof(byte[]))
                    continue;

                string name = type.Name + "." + property.Name;

                if (notPayloads.Contains(name))
                    continue;

                JsonConverterAttribute? converter = property.GetCustomAttribute<JsonConverterAttribute>();

                if (converter?.ConverterType != typeof(KeyValuePayloadJsonConverter))
                    unprotected.Add(name);
            }
        }

        Assert.True(
            unprotected.Count == 0,
            "These payload properties would flatten a null value to an empty one: " + string.Join(", ", unprotected));
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

    /// <summary>Answers the non-locating batch read with one fixed result per requested key.</summary>
    private sealed class FixedBatchGetKahuna : FakeKahunaBase
    {
        private readonly List<(KeyValueResponseType type, string key, ReadOnlyKeyValueEntry? entry)> results;

        public FixedBatchGetKahuna(List<(KeyValueResponseType type, string key, ReadOnlyKeyValueEntry? entry)> results)
            => this.results = results;

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(
            HLCTimestamp transactionId, HLCTimestamp readTimestamp,
            List<(string key, long revision, KeyValueDurability durability)> keys)
        {
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = new(results.Count);

            foreach ((KeyValueResponseType type, string key, ReadOnlyKeyValueEntry? entry) in results)
                responses.Add((type, key, KeyValueDurability.Persistent, entry));

            return Task.FromResult(responses);
        }
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


/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using Google.Protobuf.Collections;
using Kahuna.Client.Communication;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;

namespace Kahuna.Server.Tests;

/// <summary>
/// Hermetic guards for how the client encodes an outbound request. No cluster, no Docker and no
/// socket: the in-process client tests reach the server through <c>InProcessKahunaCommunication</c>,
/// so neither the REST transport nor the gRPC transport is exercised anywhere else in this project.
///
/// Three properties are pinned here:
///   • a REST body serialized straight to UTF-8 is byte-for-byte the body the old string-then-encode
///     path produced, and the content that carries it still declares application/json and can be
///     read more than once so a retry can replay it;
///   • reserving room in a repeated protobuf field changes the field's capacity only, never the items
///     it ends up holding or their order, and a sequence that cannot report its size still works;
///   • one shared Polly retry policy gives every execution its own attempt count and its own delay
///     progression — the property that lets the REST transport hold two policies for the process
///     instead of building one per call.
/// </summary>
public sealed class TestClientRequestEncoding
{
    // ── REST bodies: direct UTF-8 must equal string-then-encode, byte for byte ──────────────────

    private static void AssertSameBytes<T>(T request, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo)
    {
        byte[] viaString = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(request, typeInfo));
        byte[] direct = JsonSerializer.SerializeToUtf8Bytes(request, typeInfo);

        Assert.Equal(viaString, direct);
    }

    [Fact]
    public void SetKeyValueRequest_DirectUtf8_MatchesStringEncoding()
    {
        // A default request: every omitted-versus-default decision the source generator makes has to
        // land the same way on both paths.
        AssertSameBytes(new KahunaSetKeyValueRequest(), KahunaJsonContext.Default.KahunaSetKeyValueRequest);
    }

    [Theory]
    [InlineData("plain/key")]
    [InlineData("acentuación/clé/ключ")]      // multi-byte UTF-8 outside the ASCII range
    [InlineData("emoji/🐙/key")]               // characters above the basic multilingual plane
    [InlineData("quote\"back\\slash\nnewline")] // characters the JSON writer must escape
    public void SetKeyValueRequest_DirectUtf8_MatchesStringEncoding_ForUnicodeKeys(string key)
    {
        AssertSameBytes(
            new KahunaSetKeyValueRequest { Key = key, Value = [1, 2, 3] },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);
    }

    [Fact]
    public void SetKeyValueRequest_DirectUtf8_MatchesStringEncoding_ForNullEmptyAndBinaryValues()
    {
        // null, empty and populated must stay distinguishable, and a byte array still travels as
        // base64 on both paths.
        AssertSameBytes(
            new KahunaSetKeyValueRequest { Key = "k", Value = null },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        AssertSameBytes(
            new KahunaSetKeyValueRequest { Key = "k", Value = [] },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        byte[] binary = new byte[256];
        for (int i = 0; i < binary.Length; i++)
            binary[i] = (byte)i;

        AssertSameBytes(
            new KahunaSetKeyValueRequest { Key = "k", Value = binary, CompareValue = binary },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);
    }

    [Fact]
    public void SetKeyValueRequest_DirectUtf8_MatchesStringEncoding_ForTransactionIdentity()
    {
        // The identity fields a transactional write carries: a non-zero HLC, a coordinator key and
        // both halves of the operation id.
        AssertSameBytes(
            new KahunaSetKeyValueRequest
            {
                TransactionId = new HLCTimestamp(3, 1717171717171, 42),
                Key = "acc/1",
                Value = [9],
                CompareRevision = 7,
                ExpiresMs = 5000,
                Flags = KeyValueFlags.SetIfNotExists,
                Durability = KeyValueDurability.Persistent,
                CoordinatorKey = "coord/ordinación",
                OperationIdHigh = ulong.MaxValue,
                OperationIdLow = 1
            },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);
    }

    [Fact]
    public void ManyKeyValueRequests_DirectUtf8_MatchStringEncoding()
    {
        AssertSameBytes(
            new KahunaSetManyKeyValueRequest
            {
                Items =
                [
                    new KahunaSetKeyValueRequestItem { Key = "a", Value = [1], Durability = KeyValueDurability.Ephemeral },
                    new KahunaSetKeyValueRequestItem { Key = "b", Value = null },
                    new KahunaSetKeyValueRequestItem { Key = "b", Value = [] }   // duplicate key, empty value
                ]
            },
            KahunaJsonContext.Default.KahunaSetManyKeyValueRequest);

        AssertSameBytes(
            new KahunaDeleteManyKeyValueRequest
            {
                Items =
                [
                    new KahunaDeleteKeyValueRequestItem { Key = "a", TransactionId = new HLCTimestamp(1, 2, 3) }
                ]
            },
            KahunaJsonContext.Default.KahunaDeleteManyKeyValueRequest);
    }

    [Fact]
    public void LockRequest_DirectUtf8_MatchesStringEncoding()
    {
        AssertSameBytes(
            new KahunaLockRequest
            {
                Resource = "recurso/único",
                Owner = Encoding.UTF8.GetBytes("owner-1"),
                ExpiresMs = 10_000,
                Durability = LockDurability.Persistent
            },
            KahunaJsonContext.Default.KahunaLockRequest);
    }

    // ── The content wrapper that carries those bytes ────────────────────────────────────────────

    [Fact]
    public async Task Utf8JsonContent_DeclaresJson_AndCarriesTheExactBytes()
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(
            new KahunaSetKeyValueRequest { Key = "k/é", Value = [1, 2, 3] },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        RestCommunication.Utf8JsonContent content = new(payload);

        Assert.Equal("application/json", content.Headers.ContentType?.ToString());
        Assert.Equal(payload, await content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Utf8JsonContent_CanBeReadTwice_SoARetryCanReplayIt()
    {
        // Each attempt builds a fresh content over the same buffer, but the buffer itself must stay
        // readable: a redirect or a transport retry replays the identical body.
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(
            new KahunaSetKeyValueRequest { Key = "k", Value = [7] },
            KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        RestCommunication.Utf8JsonContent first = new(payload);
        Assert.Equal(payload, await first.ReadAsByteArrayAsync(TestContext.Current.CancellationToken));
        Assert.Equal(payload, await first.ReadAsByteArrayAsync(TestContext.Current.CancellationToken));

        RestCommunication.Utf8JsonContent second = new(payload);
        Assert.Equal(payload, await second.ReadAsByteArrayAsync(TestContext.Current.CancellationToken));
    }

    // ── Repeated protobuf request fields: reserve room without changing the contents ────────────

    [Fact]
    public void AddManyKeyValuesRequestItems_ReservesExactly_ForACountedSource()
    {
        List<KahunaGetManyKeyValuesRequestItem> source = [];
        for (int i = 0; i < 512; i++)
            source.Add(new KahunaGetManyKeyValuesRequestItem { Key = "k/" + i, Revision = i, Durability = KeyValueDurability.Persistent });

        RepeatedField<GrpcTryManyValuesRequestItem> target = [];
        GrpcCommunication.AddManyKeyValuesRequestItems(target, source);

        Assert.Equal(512, target.Count);

        // Exactly the requested room: no growth reallocation and no spare capacity left over.
        Assert.Equal(512, target.Capacity);

        for (int i = 0; i < 512; i++)
        {
            Assert.Equal("k/" + i, target[i].Key);
            Assert.Equal(i, target[i].Revision);
        }
    }

    [Fact]
    public void AddManyKeyValuesRequestItems_HandlesEmptyAndSingleSources()
    {
        RepeatedField<GrpcTryManyValuesRequestItem> empty = [];
        GrpcCommunication.AddManyKeyValuesRequestItems(empty, Array.Empty<KahunaGetManyKeyValuesRequestItem>());
        Assert.Empty(empty);

        RepeatedField<GrpcTryManyValuesRequestItem> single = [];
        GrpcCommunication.AddManyKeyValuesRequestItems(single, [new KahunaGetManyKeyValuesRequestItem { Key = "only" }]);
        Assert.Single(single);
        Assert.Equal("only", single[0].Key);
    }

    [Fact]
    public void AddManyKeyValuesRequestItems_AcceptsASourceThatCannotReportItsSize()
    {
        // A generator can only be walked once and cannot be counted first, so the field keeps its
        // incremental growth for this shape. The items must still arrive intact and in order.
        static IEnumerable<KahunaGetManyKeyValuesRequestItem> OneShot()
        {
            yield return new KahunaGetManyKeyValuesRequestItem { Key = "first" };
            yield return new KahunaGetManyKeyValuesRequestItem { Key = "second" };
            yield return new KahunaGetManyKeyValuesRequestItem { Key = "third" };
        }

        RepeatedField<GrpcTryManyValuesRequestItem> target = [];
        GrpcCommunication.AddManyKeyValuesRequestItems(target, OneShot());

        Assert.Equal(3, target.Count);
        Assert.Equal("first", target[0].Key);
        Assert.Equal("second", target[1].Key);
        Assert.Equal("third", target[2].Key);
    }

    [Fact]
    public void AddManyKeyValuesRequestItems_AppendsToAPopulatedField()
    {
        // The reservation has to account for the items already present, or the append would either
        // reallocate anyway or ask for a capacity below the current count.
        RepeatedField<GrpcTryManyValuesRequestItem> target = [];
        target.Add(new GrpcTryManyValuesRequestItem { Key = "existing" });

        GrpcCommunication.AddManyKeyValuesRequestItems(target,
        [
            new KahunaGetManyKeyValuesRequestItem { Key = "added-1" },
            new KahunaGetManyKeyValuesRequestItem { Key = "added-2" }
        ]);

        Assert.Equal(3, target.Count);

        // The reservation must never ask for less room than the field already uses: a repeated field
        // rejects a capacity below its count. It also must not shrink a field that is already larger,
        // which is why this asserts a floor rather than an exact figure.
        Assert.True(target.Capacity >= 3);

        Assert.Equal("existing", target[0].Key);
        Assert.Equal("added-1", target[1].Key);
        Assert.Equal("added-2", target[2].Key);
    }

    [Fact]
    public void AddSetManyKeyValueRequestItems_ReservesExactly_AndPreservesEveryField()
    {
        // This case is about the reservation and field mapping only. What an absent value means on the
        // wire is pinned separately, in TestKeyValuePayloadPresence.
        List<KahunaSetKeyValueRequestItem> source =
        [
            new() { Key = "a", Value = [1, 2], ExpiresMs = 10, Durability = KeyValueDurability.Ephemeral },
            new() { Key = "b", Value = [3], Durability = KeyValueDurability.Persistent },
            new() { Key = "a", Value = [], Flags = KeyValueFlags.SetIfNotExists },  // duplicate key, empty value
            new() { Key = "c", Value = null }
        ];

        RepeatedField<GrpcTrySetManyKeyValueRequestItem> target = [];
        GrpcCommunication.AddSetManyKeyValueRequestItems(target, source);

        Assert.Equal(4, target.Count);
        Assert.Equal(4, target.Capacity);

        Assert.Equal("a", target[0].Key);
        Assert.Equal(10, target[0].ExpiresMs);
        Assert.Equal((int)KeyValueDurability.Ephemeral, (int)target[0].Durability);
        Assert.Equal([1, 2], target[0].Value.ToByteArray());

        // A duplicate key must survive as its own item, and an empty value must stay present but empty.
        Assert.Equal("a", target[2].Key);
        Assert.True(target[2].HasValue);
        Assert.Empty(target[2].Value.ToByteArray());

        // A null value must reserve room and encode as an absent field, not throw and not become empty.
        Assert.Equal("c", target[3].Key);
        Assert.False(target[3].HasValue);
    }

    [Fact]
    public void AddDeleteManyKeyValueRequestItems_ReservesExactly_AndPreservesOrder()
    {
        List<KahunaDeleteKeyValueRequestItem> source =
        [
            new() { Key = "z", TransactionId = new HLCTimestamp(1, 2, 3) },
            new() { Key = "a", TransactionId = new HLCTimestamp(4, 5, 6) }
        ];

        RepeatedField<GrpcTryDeleteManyKeyValueRequestItem> target = [];
        GrpcCommunication.AddDeleteManyKeyValueRequestItems(target, source);

        Assert.Equal(2, target.Count);
        Assert.Equal(2, target.Capacity);

        // Input order is the response order the caller matches against, so it must not be sorted.
        Assert.Equal("z", target[0].Key);
        Assert.Equal("a", target[1].Key);
        Assert.Equal(1, target[0].TransactionIdNode);
        Assert.Equal(6u, target[1].TransactionIdCounter);
    }

    [Fact]
    public void AddTransactionParameters_ReservesExactly_AndKeepsANullValueAbsent()
    {
        List<KeyValueParameter> parameters =
        [
            new() { Key = "@a", Value = "1" },
            new() { Key = "@b", Value = null }
        ];

        RepeatedField<GrpcKeyValueParameter> target = [];
        GrpcCommunication.AddTransactionParameters(target, parameters);

        Assert.Equal(2, target.Count);
        Assert.Equal(2, target.Capacity);
        Assert.Equal("@a", target[0].Key);
        Assert.Equal("1", target[0].Value);
        Assert.False(target[1].HasValue);
    }

    [Fact]
    public void AddTransactionParameters_HandlesAnEmptyList()
    {
        RepeatedField<GrpcKeyValueParameter> target = [];
        GrpcCommunication.AddTransactionParameters(target, []);

        Assert.Empty(target);
    }

    // ── One shared retry policy must not couple its executions ──────────────────────────────────

    /// <summary>
    /// The REST transport holds two retry policies for the whole process rather than building one per
    /// call. That is only sound while a Polly policy stays stateless across executions: each execution
    /// must take its own enumerator over the jitter sequence. This test pins that dependency
    /// behaviour, so a Polly upgrade that changed it would fail here instead of silently making every
    /// caller retry in lockstep.
    /// </summary>
    [Fact]
    public async Task OneSharedRetryPolicy_GivesEachExecutionItsOwnAttemptsAndDelays()
    {
        List<TimeSpan> first = [];
        List<TimeSpan> second = [];

        AsyncRetryPolicy shared = Policy
            .Handle<InvalidOperationException>()
            .WaitAndRetryAsync(
                Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: 5),
                (_, delay, context) => ((List<TimeSpan>)context["sink"]).Add(delay));

        async Task<int> Drive(List<TimeSpan> sink, int failures)
        {
            int attempts = 0;
            Context context = new() { ["sink"] = sink };

            await shared.ExecuteAsync(_ =>
            {
                attempts++;
                if (attempts <= failures)
                    throw new InvalidOperationException("transient");

                return Task.CompletedTask;
            }, context);

            return attempts;
        }

        // Run them concurrently on the one policy instance: a shared enumerator would interleave.
        int[] attempts = await Task.WhenAll(Drive(first, 4), Drive(second, 2));

        Assert.Equal(5, attempts[0]);
        Assert.Equal(3, attempts[1]);
        Assert.Equal(4, first.Count);
        Assert.Equal(2, second.Count);

        // Independent progressions: the two executions must not have walked one shared sequence.
        Assert.NotEqual(first.Take(2), second.Take(2));
    }
}

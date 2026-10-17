using Google.Protobuf;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The inter-node encoding of a batched probe: a forwarded probe must decode to exactly the answers the serving node
/// produced — entry presence, an empty value apart from no value, the holder of a refusal, and the lock flag.
/// </summary>
public sealed class TestProbeManyWire
{
    [Fact]
    public void Answers_RoundTrip_EveryField()
    {
        HLCTimestamp holder = new(2, 500, 3);
        KeyValueProbeResult[] results =
        [
            new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry([1, 2, 3], 7, new(1, 10, 0), new(1, 20, 1), new(1, 30, 2), KeyValueState.Set), HLCTimestamp.Zero, true),
            new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry([], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, new(1, 5, 0), KeyValueState.Set), HLCTimestamp.Zero, false),
            new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueEntry(null, 0, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Undefined), HLCTimestamp.Zero, true),
            new(KeyValueResponseType.AlreadyLocked, null, holder, false),
            KeyValueProbeResult.MustRetry
        ];

        GrpcTryProbeManyResponse response = new();
        ProbeManyWire.AddItems(response.Items, results);
        GrpcTryProbeManyResponse parsed = GrpcTryProbeManyResponse.Parser.ParseFrom(response.ToByteArray());

        KeyValueProbeResult[] decoded = ProbeManyWire.FromItems(parsed.Items, results.Length);

        for (int i = 0; i < results.Length; i++)
        {
            Assert.Equal(results[i].Type, decoded[i].Type);
            Assert.Equal(results[i].HolderTransactionId, decoded[i].HolderTransactionId);
            Assert.Equal(results[i].Locked, decoded[i].Locked);
            Assert.Equal(results[i].Entry is null, decoded[i].Entry is null);

            if (results[i].Entry is not { } expected)
                continue;

            ReadOnlyKeyValueEntry actual = decoded[i].Entry!;
            Assert.Equal(expected.Value, actual.Value);
            Assert.Equal(expected.Revision, actual.Revision);
            Assert.Equal(expected.Expires, actual.Expires);
            Assert.Equal(expected.LastUsed, actual.LastUsed);
            Assert.Equal(expected.LastModified, actual.LastModified);
            Assert.Equal(expected.State, actual.State);
        }

        Assert.NotNull(decoded[1].Entry!.Value);
        Assert.Empty(decoded[1].Entry!.Value!);
        Assert.Null(decoded[2].Entry!.Value);
    }

    [Fact]
    public void MisalignedAnswer_IsMustRetryForEveryKey_NeverShifted()
    {
        GrpcTryProbeManyResponse response = new();
        ProbeManyWire.AddItems(response.Items, [new(KeyValueResponseType.Get, null, HLCTimestamp.Zero, true)]);

        KeyValueProbeResult[] decoded = ProbeManyWire.FromItems(response.Items, 2);

        Assert.All(decoded, r =>
        {
            Assert.Equal(KeyValueResponseType.MustRetry, r.Type);
            Assert.False(r.Locked);
        });
    }

    [Fact]
    public void Request_RoundTrips()
    {
        HLCTimestamp tx = new(1, 100, 2);
        HLCTimestamp readAt = new(1, 90, 0);

        GrpcTryProbeManyRequest request = ProbeManyWire.ToRequest(tx, readAt, ["s/a", "s/b"], KeyValueProbeLock.SharedReentryOnly, 1500, KeyValueDurability.Ephemeral);
        GrpcTryProbeManyRequest parsed = GrpcTryProbeManyRequest.Parser.ParseFrom(request.ToByteArray());

        Assert.Equal(["s/a", "s/b"], parsed.Keys);
        Assert.Equal(tx, new HLCTimestamp(parsed.TransactionIdNode, parsed.TransactionIdPhysical, parsed.TransactionIdCounter));
        Assert.Equal(readAt, new HLCTimestamp(parsed.ReadTimestampNode, parsed.ReadTimestampPhysical, parsed.ReadTimestampCounter));
        Assert.Equal(KeyValueProbeLock.SharedReentryOnly, (KeyValueProbeLock)parsed.LockMode);
        Assert.Equal(1500, parsed.ExpiresMs);
        Assert.Equal(KeyValueDurability.Ephemeral, (KeyValueDurability)parsed.Durability);

        GrpcTryProbeManyResponse refused = ProbeManyWire.MustRetry(parsed);
        Assert.Equal(2, refused.Items.Count);
        Assert.All(refused.Items, i => Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)i.Type));
    }
}

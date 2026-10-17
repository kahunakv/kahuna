using Google.Protobuf;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The inter-node encoding of a batched range probe: a forwarded probe must carry exactly the seeks the caller asked
/// for, open bounds included, and decode to exactly the answers the serving node produced — every entry, an empty
/// value apart from no value, the resume key, the holder of a refusal, and the lock flag.
/// </summary>
public sealed class TestProbeManyRangesWire
{
    [Fact]
    public void Request_RoundTrips_OpenAndClosedBounds()
    {
        KeyValueProbeRange[] seeks =
        [
            new("s/a", true, "s/b", false, 11),
            new(null, false, "s/c", true, 1),
            new("s/d", false, null, false, 512),
            new(null, true, null, true, 7),
            new("", true, "", false, 3)
        ];

        GrpcTryProbeManyRangesRequest request = ProbeManyRangesWire.ToRequest(
            new HLCTimestamp(1, 2, 3), new HLCTimestamp(4, 5, 6), "s", seeks, KeyValueProbeLock.Shared, 1_500, KeyValueDurability.Persistent);
        GrpcTryProbeManyRangesRequest parsed = GrpcTryProbeManyRangesRequest.Parser.ParseFrom(request.ToByteArray());

        Assert.Equal(seeks, ProbeManyRangesWire.FromRequest(parsed));
        Assert.Equal("s", parsed.Prefix);
        Assert.Equal(GrpcProbeLockMode.GrpcProbeLockShared, parsed.LockMode);
        Assert.Equal(1_500, parsed.ExpiresMs);
        Assert.Equal(new HLCTimestamp(1, 2, 3), new HLCTimestamp(parsed.TransactionIdNode, parsed.TransactionIdPhysical, parsed.TransactionIdCounter));
        Assert.Equal(new HLCTimestamp(4, 5, 6), new HLCTimestamp(parsed.ReadTimestampNode, parsed.ReadTimestampPhysical, parsed.ReadTimestampCounter));
    }

    [Fact]
    public void Answers_RoundTrip_EveryField()
    {
        HLCTimestamp holder = new(2, 500, 3);
        KeyValueProbeRangeResult[] results =
        [
            new(KeyValueResponseType.Get,
                [
                    ("s/a1", new ReadOnlyKeyValueEntry([1, 2, 3], 7, new(1, 10, 0), new(1, 20, 1), new(1, 30, 2), KeyValueState.Set)),
                    ("s/a2", new ReadOnlyKeyValueEntry([], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, new(1, 5, 0), KeyValueState.Set)),
                    ("s/a3", new ReadOnlyKeyValueEntry(null, 2, HLCTimestamp.Zero, HLCTimestamp.Zero, new(1, 6, 0), KeyValueState.Set))
                ],
                true, "s/a3", HLCTimestamp.Zero, true),
            new(KeyValueResponseType.Get, [], false, null, HLCTimestamp.Zero, false),
            new(KeyValueResponseType.AlreadyLocked, [], false, null, holder, false),
            KeyValueProbeRangeResult.MustRetry with { Locked = true }
        ];

        GrpcTryProbeManyRangesResponse response = new();
        ProbeManyRangesWire.AddItems(response.Items, results);
        GrpcTryProbeManyRangesResponse parsed = GrpcTryProbeManyRangesResponse.Parser.ParseFrom(response.ToByteArray());

        KeyValueProbeRangeResult[] decoded = ProbeManyRangesWire.FromItems(parsed.Items, results.Length);

        for (int i = 0; i < results.Length; i++)
        {
            Assert.Equal(results[i].Type, decoded[i].Type);
            Assert.Equal(results[i].HasMore, decoded[i].HasMore);
            Assert.Equal(results[i].ResumeKey, decoded[i].ResumeKey);
            Assert.Equal(results[i].HolderTransactionId, decoded[i].HolderTransactionId);
            Assert.Equal(results[i].Locked, decoded[i].Locked);
            Assert.Equal(results[i].Items.Count, decoded[i].Items.Count);

            for (int j = 0; j < results[i].Items.Count; j++)
            {
                (string key, ReadOnlyKeyValueEntry expected) = results[i].Items[j];
                (string decodedKey, ReadOnlyKeyValueEntry actual) = decoded[i].Items[j];

                Assert.Equal(key, decodedKey);
                Assert.Equal(expected.Value, actual.Value);
                Assert.Equal(expected.Revision, actual.Revision);
                Assert.Equal(expected.Expires, actual.Expires);
                Assert.Equal(expected.LastUsed, actual.LastUsed);
                Assert.Equal(expected.LastModified, actual.LastModified);
                Assert.Equal(expected.State, actual.State);
            }
        }

        Assert.NotNull(decoded[0].Items[1].Item2.Value);
        Assert.Empty(decoded[0].Items[1].Item2.Value!);
        Assert.Null(decoded[0].Items[2].Item2.Value);
    }

    [Fact]
    public void MisalignedAnswer_IsMustRetryForEverySeek_NeverShifted()
    {
        GrpcTryProbeManyRangesResponse response = new();
        ProbeManyRangesWire.AddItems(response.Items, [new(KeyValueResponseType.Get, [], false, null, HLCTimestamp.Zero, true)]);

        KeyValueProbeRangeResult[] decoded = ProbeManyRangesWire.FromItems(response.Items, 3);

        Assert.Equal(3, decoded.Length);
        Assert.All(decoded, r =>
        {
            Assert.Equal(KeyValueResponseType.MustRetry, r.Type);
            Assert.False(r.Locked);
            Assert.Empty(r.Items);
        });
    }

    [Fact]
    public void Refusal_AnswersEveryRequestedSeek()
    {
        GrpcTryProbeManyRangesRequest request = ProbeManyRangesWire.ToRequest(
            HLCTimestamp.Zero, HLCTimestamp.Zero, "s", [new("s/a", true, "s/b", true, 1), new("s/c", true, "s/d", true, 2)],
            KeyValueProbeLock.None, 0, KeyValueDurability.Persistent);

        GrpcTryProbeManyRangesResponse refused = ProbeManyRangesWire.MustRetry(request);
        KeyValueProbeRangeResult[] decoded = ProbeManyRangesWire.FromItems(refused.Items, 2);

        Assert.All(decoded, r => Assert.Equal(KeyValueResponseType.MustRetry, r.Type));
    }
}

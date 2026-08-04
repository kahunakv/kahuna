using Google.Protobuf;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A registered operation completion forwarded to a remote coordinator must survive the inter-node gRPC
/// wire with every effect-bearing field intact. A field that maps on one side but not the other silently
/// narrows the coordinator's working set — most dangerously the held-lock set, whose loss makes commit
/// skip the transaction's mutations entirely while still reporting Committed. These tests round-trip a
/// fully-populated payload through the real protobuf encoding (sender mapping → bytes → parse → receiver
/// mapping) so any dropped field fails loudly here.
/// </summary>
public sealed class TestCompleteOperationGrpcWire
{
    private static OperationCompletionPayload RoundTrip(OperationCompletionPayload payload)
    {
        GrpcCompleteOperationRequest request = GrpcInterNodeCommunication.ToGrpcCompleteOperationRequest(
            "coord-key", new HLCTimestamp(1, 200, 3), new TransactionOperationId(7, 9), payload);

        GrpcCompleteOperationRequest onWire = GrpcCompleteOperationRequest.Parser.ParseFrom(request.ToByteArray());

        return KeyValuesService.FromGrpcCompleteOperationRequest(onWire);
    }

    // StagedMutationEffect is a record struct over a byte[] value, whose default equality compares array
    // references — compare the value bytes explicitly.
    private static void AssertStagedMutationsEqual(IReadOnlyList<StagedMutationEffect> expected, IReadOnlyList<StagedMutationEffect>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Count, actual!.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Key, actual[i].Key);
            Assert.Equal(expected[i].Value, actual[i].Value);
            Assert.Equal(expected[i].Revision, actual[i].Revision);
            Assert.Equal(expected[i].ExpiresMs, actual[i].ExpiresMs);
            Assert.Equal(expected[i].NoRevision, actual[i].NoRevision);
        }
    }

    [Fact]
    public void FullyPopulatedPayload_RoundTrips_EveryField()
    {
        OperationCompletionPayload payload = new()
        {
            ModifiedKey = "single-key",
            ModifiedKeys = [("batch-a", KeyValueDurability.Persistent), ("batch-b", KeyValueDurability.Ephemeral)],
            AcquiredPointLock = "lock-single",
            AcquiredPointLocks = [("batch-a", KeyValueDurability.Persistent), ("batch-b", KeyValueDurability.Ephemeral)],
            ReleasedPointLock = "lock-released",
            AcquiredPrefixLock = "prefix-acquired",
            ReleasedPrefixLock = "prefix-released",
            AcquiredRangeLock = (new RangeLockKey("pfx", "start", true, "end", false, KeyValueDurability.Persistent), RangeLockMode.Shared),
            ReleasedRangeLock = new RangeLockKey("pfx2", null, false, null, true, KeyValueDurability.Ephemeral),
            Read = new() { Key = "read-key", Durability = KeyValueDurability.Persistent, Exists = true, Revision = 11 },
            ReadObservations =
            [
                new() { Key = "obs-1", Durability = KeyValueDurability.Persistent, Exists = true, Revision = 5 },
                new() { Key = "obs-2", Durability = KeyValueDurability.Ephemeral, Exists = false, Revision = -1 }
            ],
            StagedMutations =
            [
                new("batch-a", "v"u8.ToArray(), 6, 30_000, false),
                new("batch-b", null, 7, 0, true)
            ],
            Durability = KeyValueDurability.Persistent,
            CachedType = KeyValueResponseType.Set,
            CachedRevision = 42,
            CachedTimestamp = new HLCTimestamp(2, 300, 4)
        };

        OperationCompletionPayload restored = RoundTrip(payload);

        Assert.Equal(payload.ModifiedKey, restored.ModifiedKey);
        Assert.Equal(payload.ModifiedKeys, restored.ModifiedKeys);
        Assert.Equal(payload.AcquiredPointLock, restored.AcquiredPointLock);
        Assert.Equal(payload.AcquiredPointLocks, restored.AcquiredPointLocks);
        Assert.Equal(payload.ReleasedPointLock, restored.ReleasedPointLock);
        Assert.Equal(payload.AcquiredPrefixLock, restored.AcquiredPrefixLock);
        Assert.Equal(payload.ReleasedPrefixLock, restored.ReleasedPrefixLock);
        Assert.Equal(payload.AcquiredRangeLock!.Value.Range, restored.AcquiredRangeLock!.Value.Range);
        Assert.Equal(payload.AcquiredRangeLock!.Value.Mode, restored.AcquiredRangeLock!.Value.Mode);
        Assert.Equal(payload.ReleasedRangeLock, restored.ReleasedRangeLock);
        Assert.NotNull(restored.Read);
        Assert.Equal(payload.Read!.Key, restored.Read!.Key);
        Assert.Equal(payload.Read.Revision, restored.Read.Revision);
        Assert.NotNull(restored.ReadObservations);
        Assert.Equal(payload.ReadObservations!.Count, restored.ReadObservations!.Count);
        AssertStagedMutationsEqual(payload.StagedMutations!, restored.StagedMutations);
        Assert.Equal(payload.Durability, restored.Durability);
        Assert.Equal(payload.CachedType, restored.CachedType);
        Assert.Equal(payload.CachedRevision, restored.CachedRevision);
        Assert.Equal(payload.CachedTimestamp, restored.CachedTimestamp);
    }

    /// <summary>
    /// The batch write/lock paths carry the held-lock set only in the plural field; an optimistic
    /// transaction has no other source of it, so losing it on the wire silently drops the commit.
    /// </summary>
    [Fact]
    public void BatchHeldLockSet_SurvivesTheWire()
    {
        OperationCompletionPayload payload = new()
        {
            ModifiedKeys = [("k1", KeyValueDurability.Persistent)],
            StagedMutations = [new("k1", "v1"u8.ToArray(), 1, 0, false)],
            AcquiredPointLocks = [("k1", KeyValueDurability.Persistent)],
            Durability = KeyValueDurability.Persistent,
            CachedType = KeyValueResponseType.Set
        };

        OperationCompletionPayload restored = RoundTrip(payload);

        Assert.NotNull(restored.AcquiredPointLocks);
        Assert.Equal(payload.AcquiredPointLocks!, restored.AcquiredPointLocks!);
    }

    [Fact]
    public void EmptyOptionalFields_StayNull()
    {
        OperationCompletionPayload payload = new()
        {
            Durability = KeyValueDurability.Ephemeral,
            CachedType = KeyValueResponseType.MustRetry
        };

        OperationCompletionPayload restored = RoundTrip(payload);

        Assert.Null(restored.ModifiedKey);
        Assert.Null(restored.ModifiedKeys);
        Assert.Null(restored.AcquiredPointLock);
        Assert.Null(restored.AcquiredPointLocks);
        Assert.Null(restored.ReleasedPointLock);
        Assert.Null(restored.AcquiredPrefixLock);
        Assert.Null(restored.ReleasedPrefixLock);
        Assert.Null(restored.AcquiredRangeLock);
        Assert.Null(restored.ReleasedRangeLock);
        Assert.Null(restored.Read);
        Assert.Null(restored.ReadObservations);
        Assert.Null(restored.StagedMutations);
    }
}

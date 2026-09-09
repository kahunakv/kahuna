using Google.Protobuf;
using Google.Protobuf.Collections;
using Kahuna.Server.Communication.Internode;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The inter-node bulk encoders populate a repeated field directly and reserve its capacity from the
/// input count, instead of feeding an iterator to the field. These tests pin two contracts: the
/// encoded items are byte-identical to a per-item construction of the same batch (order, every field,
/// and the null-vs-empty presence bit for optional payloads), and the reservation sizes the field
/// exactly once, including when the encoder appends to a field that already holds items.
/// </summary>
public sealed class TestInterNodeBulkRequestEncoding
{
    private static GrpcTrySetManyKeyValueRequestItem ExpectedSetItem(KahunaSetKeyValueRequestItem item)
    {
        GrpcTrySetManyKeyValueRequestItem expected = new()
        {
            TransactionIdNode = item.TransactionId.N,
            TransactionIdPhysical = item.TransactionId.L,
            TransactionIdCounter = item.TransactionId.C,
            Key = item.Key,
            CompareRevision = item.CompareRevision,
            Flags = (GrpcKeyValueFlags)item.Flags,
            ExpiresMs = item.ExpiresMs,
            Durability = (GrpcKeyValueDurability)item.Durability,
            RoutedGeneration = item.RoutedGeneration,
        };

        if (item.Value is not null)
            expected.Value = ByteString.CopyFrom(item.Value);

        if (item.CompareValue is not null)
            expected.CompareValue = ByteString.CopyFrom(item.CompareValue);

        return expected;
    }

    private static List<KahunaSetKeyValueRequestItem> BuildSetItems(int count)
    {
        List<KahunaSetKeyValueRequestItem> items = new(count);

        for (int i = 0; i < count; i++)
            items.Add(new KahunaSetKeyValueRequestItem
            {
                TransactionId = new HLCTimestamp(1, 100 + i, (uint)i),
                Key = $"key/{i}",
                // Cycle a null payload, an empty payload, and a non-empty payload so every
                // presence-bit shape appears in every batch of three or more items.
                Value = (i % 3) switch { 0 => null, 1 => [], _ => [(byte)i, 0x7F] },
                CompareValue = i % 2 == 0 ? null : [(byte)(i + 1)],
                CompareRevision = i,
                ExpiresMs = 1000 + i,
                Flags = KeyValueFlags.Set,
                Durability = i % 2 == 0 ? KeyValueDurability.Persistent : KeyValueDurability.Ephemeral,
                RoutedGeneration = i,
            });

        return items;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(64)]
    [InlineData(512)]
    public void SetMany_EncodesIdenticalBytes_AndReservesExactCapacity(int count)
    {
        List<KahunaSetKeyValueRequestItem> items = BuildSetItems(count);

        GrpcTrySetManyKeyValueRequest expected = new();
        foreach (KahunaSetKeyValueRequestItem item in items)
            expected.Items.Add(ExpectedSetItem(item));

        GrpcTrySetManyKeyValueRequest actual = new();
        GrpcInterNodeCommunication.AddSetManyRequestItems(actual.Items, items);

        Assert.Equal(count, actual.Items.Count);
        Assert.Equal(expected, actual);
        Assert.Equal(expected.ToByteArray(), actual.ToByteArray());

        // An empty batch must not reserve anything; a non-empty batch must reserve the exact count,
        // which also proves no add grew the field past the reservation.
        Assert.Equal(count, actual.Items.Capacity);
    }

    [Fact]
    public void SetMany_PreservesNullVersusEmptyPayloadPresence()
    {
        List<KahunaSetKeyValueRequestItem> items = BuildSetItems(3);

        GrpcTrySetManyKeyValueRequest request = new();
        GrpcInterNodeCommunication.AddSetManyRequestItems(request.Items, items);

        GrpcTrySetManyKeyValueRequest onWire = GrpcTrySetManyKeyValueRequest.Parser.ParseFrom(request.ToByteArray());

        Assert.False(onWire.Items[0].HasValue);  // null payload stays absent
        Assert.True(onWire.Items[1].HasValue);   // empty payload stays present
        Assert.Empty(onWire.Items[1].Value);
        Assert.True(onWire.Items[2].HasValue);
        Assert.Equal(2, onWire.Items[2].Value.Length);
    }

    [Fact]
    public void SetMany_AppendsAfterExistingItems()
    {
        GrpcTrySetManyKeyValueRequest request = new();
        GrpcInterNodeCommunication.AddSetManyRequestItems(request.Items, BuildSetItems(2));
        GrpcInterNodeCommunication.AddSetManyRequestItems(request.Items, BuildSetItems(3));

        Assert.Equal(5, request.Items.Count);
        Assert.Equal(5, request.Items.Capacity);
        Assert.Equal("key/0", request.Items[0].Key);
        Assert.Equal("key/2", request.Items[4].Key);
    }

    [Fact]
    public void DeleteMany_EncodesIdenticalBytes()
    {
        List<KahunaDeleteKeyValueRequestItem> items = [];
        for (int i = 0; i < 8; i++)
            items.Add(new KahunaDeleteKeyValueRequestItem
            {
                TransactionId = new HLCTimestamp(2, 200 + i, (uint)i),
                Key = $"del/{i}",
                Durability = i % 2 == 0 ? KeyValueDurability.Persistent : KeyValueDurability.Ephemeral
            });

        GrpcTryDeleteManyKeyValueRequest expected = new();
        foreach (KahunaDeleteKeyValueRequestItem item in items)
            expected.Items.Add(new GrpcTryDeleteManyKeyValueRequestItem
            {
                TransactionIdNode = item.TransactionId.N,
                TransactionIdPhysical = item.TransactionId.L,
                TransactionIdCounter = item.TransactionId.C,
                Key = item.Key,
                Durability = (GrpcKeyValueDurability)item.Durability
            });

        GrpcTryDeleteManyKeyValueRequest actual = new();
        GrpcInterNodeCommunication.AddDeleteManyRequestItems(actual.Items, items);

        Assert.Equal(expected.ToByteArray(), actual.ToByteArray());
        Assert.Equal(items.Count, actual.Items.Capacity);
    }

    [Fact]
    public void ManyValues_EncodesIdenticalBytes()
    {
        List<(string key, long revision, KeyValueDurability durability)> keys = [];
        for (int i = 0; i < 8; i++)
            keys.Add(($"read/{i}", i, i % 2 == 0 ? KeyValueDurability.Persistent : KeyValueDurability.Ephemeral));

        GrpcTryGetManyValuesRequest expected = new();
        foreach ((string key, long revision, KeyValueDurability durability) in keys)
            expected.Items.Add(new GrpcTryManyValuesRequestItem
            {
                Key = key,
                Revision = revision,
                Durability = (GrpcKeyValueDurability)durability
            });

        GrpcTryGetManyValuesRequest actual = new();
        GrpcInterNodeCommunication.AddTryManyValuesRequestItems(actual.Items, keys);

        Assert.Equal(expected.ToByteArray(), actual.ToByteArray());
        Assert.Equal(keys.Count, actual.Items.Capacity);
    }

    [Fact]
    public void AcquireAndReleaseLocks_EncodeIdenticalBytes()
    {
        List<(string key, int expiresMs, KeyValueDurability durability)> acquire = [];
        List<(string key, KeyValueDurability durability)> release = [];
        for (int i = 0; i < 8; i++)
        {
            acquire.Add(($"lock/{i}", 500 + i, KeyValueDurability.Persistent));
            release.Add(($"lock/{i}", KeyValueDurability.Persistent));
        }

        GrpcTryAcquireManyExclusiveLocksRequest expectedAcquire = new();
        foreach ((string key, int expiresMs, KeyValueDurability durability) in acquire)
            expectedAcquire.Items.Add(new GrpcTryAcquireManyExclusiveLocksRequestItem
            {
                Key = key,
                ExpiresMs = expiresMs,
                Durability = (GrpcKeyValueDurability)durability
            });

        GrpcTryAcquireManyExclusiveLocksRequest actualAcquire = new();
        GrpcInterNodeCommunication.AddAcquireLockRequestItems(actualAcquire.Items, acquire);

        Assert.Equal(expectedAcquire.ToByteArray(), actualAcquire.ToByteArray());
        Assert.Equal(acquire.Count, actualAcquire.Items.Capacity);

        GrpcTryReleaseManyExclusiveLocksRequest expectedRelease = new();
        foreach ((string key, KeyValueDurability durability) in release)
            expectedRelease.Items.Add(new GrpcTryReleaseManyExclusiveLocksRequestItem
            {
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            });

        GrpcTryReleaseManyExclusiveLocksRequest actualRelease = new();
        GrpcInterNodeCommunication.AddReleaseLockRequestItems(actualRelease.Items, release);

        Assert.Equal(expectedRelease.ToByteArray(), actualRelease.ToByteArray());
        Assert.Equal(release.Count, actualRelease.Items.Capacity);
    }

    [Fact]
    public void PrepareCommitRollback_EncodeIdenticalBytes()
    {
        List<(string key, KeyValueDurability durability)> prepare = [];
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> ticketed = [];
        for (int i = 0; i < 8; i++)
        {
            prepare.Add(($"tx/{i}", KeyValueDurability.Persistent));
            ticketed.Add(($"tx/{i}", new HLCTimestamp(3, 300 + i, (uint)i), KeyValueDurability.Persistent));
        }

        GrpcTryPrepareManyMutationsRequest expectedPrepare = new();
        foreach ((string key, KeyValueDurability durability) in prepare)
            expectedPrepare.Items.Add(new GrpcTryPrepareManyMutationsRequestItem
            {
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            });

        GrpcTryPrepareManyMutationsRequest actualPrepare = new();
        GrpcInterNodeCommunication.AddPrepareRequestItems(actualPrepare.Items, prepare);

        Assert.Equal(expectedPrepare.ToByteArray(), actualPrepare.ToByteArray());
        Assert.Equal(prepare.Count, actualPrepare.Items.Capacity);

        GrpcTryCommitManyMutationsRequest expectedCommit = new();
        GrpcTryRollbackManyMutationsRequest expectedRollback = new();
        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) in ticketed)
        {
            expectedCommit.Items.Add(new GrpcTryCommitManyMutationsRequestItem
            {
                Key = key,
                ProposalTicketNode = ticketId.N,
                ProposalTicketPhysical = ticketId.L,
                ProposalTicketCounter = ticketId.C,
                Durability = (GrpcKeyValueDurability)durability
            });
            expectedRollback.Items.Add(new GrpcTryRollbackManyMutationsRequestItem
            {
                Key = key,
                ProposalTicketNode = ticketId.N,
                ProposalTicketPhysical = ticketId.L,
                ProposalTicketCounter = ticketId.C,
                Durability = (GrpcKeyValueDurability)durability
            });
        }

        GrpcTryCommitManyMutationsRequest actualCommit = new();
        GrpcInterNodeCommunication.AddCommitRequestItems(actualCommit.Items, ticketed);

        Assert.Equal(expectedCommit.ToByteArray(), actualCommit.ToByteArray());
        Assert.Equal(ticketed.Count, actualCommit.Items.Capacity);

        GrpcTryRollbackManyMutationsRequest actualRollback = new();
        GrpcInterNodeCommunication.AddRollbackRequestItems(actualRollback.Items, ticketed);

        Assert.Equal(expectedRollback.ToByteArray(), actualRollback.ToByteArray());
        Assert.Equal(ticketed.Count, actualRollback.Items.Capacity);
    }
}

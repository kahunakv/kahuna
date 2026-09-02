using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards the reused <see cref="KeyValueMessage"/> shells on the hot serialize/parse paths. Proto3 omits
/// default-valued fields from the wire and a merge into a dirty instance keeps every omitted field, so a
/// reused shell that is not fully reset would leak one record's fields into the next (a delete inheriting
/// the prior set's value, a non-transactional write inheriting the prior transaction id). These tests parse
/// or serialize a sparse record immediately after a fully-populated one and require the result to be
/// identical to a fresh, single-use message. The empty-payload test uses Protobuf equality over every field
/// including presence, so a field added to the proto but missed by the reset fails it automatically.
/// </summary>
public sealed class TestKeyValueMessageReuse
{
    /// <summary>Populates every field of the log record, including all presence-tracked optionals.</summary>
    private static KeyValueMessage FullyPopulated() => new()
    {
        Type = (int)KeyValueRequestType.TrySet,
        Key = "space/full",
        Value = ByteString.CopyFromUtf8("payload"),
        Revision = 42,
        ExpireNode = 1,
        ExpirePhysical = 2,
        ExpireCounter = 3,
        LastUsedNode = 4,
        LastUsedPhysical = 5,
        LastUsedCounter = 6,
        LastModifiedNode = 7,
        LastModifiedPhysical = 8,
        LastModifiedCounter = 9,
        TimeNode = 10,
        TimePhysical = 11,
        TimeCounter = 12,
        NoRevision = true,
        TransactionIdNode = 13,
        TransactionIdPhysical = 14,
        TransactionIdCounter = 15,
        RecordAnchorKey = "space/anchor",
        EmbeddedDecision = ByteString.CopyFromUtf8("decision"),
        Epoch = 16
    };

    /// <summary>
    /// After a fully-populated parse, a parse of an empty payload must equal a pristine message.
    /// Protobuf equality covers every field and its presence bit, so this fails for any field the
    /// reset misses — including fields added to the proto later.
    /// </summary>
    [Fact]
    public void ThreadCachedParse_EmptyPayloadAfterFull_EqualsPristineMessage()
    {
        byte[] full = ReplicationSerializer.Serialize(FullyPopulated());

        ReplicationSerializer.UnserializeKeyValueMessageThreadCached(full);
        KeyValueMessage reused = ReplicationSerializer.UnserializeKeyValueMessageThreadCached(ReadOnlySpan<byte>.Empty);

        Assert.Equal(new KeyValueMessage(), reused);
    }

    /// <summary>
    /// A sparse record parsed right after a fully-populated one must match a fresh parse of the same
    /// bytes: no field of the earlier record may survive into it.
    /// </summary>
    [Fact]
    public void ThreadCachedParse_SparseAfterFull_MatchesFreshParse()
    {
        byte[] full = ReplicationSerializer.Serialize(FullyPopulated());
        byte[] sparse = ReplicationSerializer.Serialize(new KeyValueMessage
        {
            Type = (int)KeyValueRequestType.TryDelete,
            Key = "space/sparse",
            Revision = 7
        });

        ReplicationSerializer.UnserializeKeyValueMessageThreadCached(full);
        KeyValueMessage reused = ReplicationSerializer.UnserializeKeyValueMessageThreadCached(sparse);

        Assert.Equal(ReplicationSerializer.UnserializeKeyValueMessage(sparse), reused);

        Assert.False(reused.HasValue);
        Assert.False(reused.HasNoRevision);
        Assert.False(reused.HasRecordAnchorKey);
        Assert.False(reused.HasEmbeddedDecision);
        Assert.Equal(0, reused.TransactionIdNode);
    }

    private static readonly HLCTimestamp Expires = new(1, 5_000_000, 7);
    private static readonly HLCTimestamp LastUsed = new(1, 6_000_000, 8);
    private static readonly HLCTimestamp LastModified = new(1, 6_500_000, 9);
    private static readonly HLCTimestamp Now = new(2, 7_000_000, 11);

    private static KeyValueProposal Proposal(KeyValueRequestType type, byte[]? value, KeyValueState state) =>
        new(type, "space/k", value, revision: 42, noRevision: false, Expires, LastUsed, LastModified, state, KeyValueDurability.Persistent);

    /// <summary>
    /// A value-less record (delete) serialized right after a valued one must not carry the previous
    /// value: its bytes must equal a fresh, single-use serialization of the same record.
    /// </summary>
    [Fact]
    public void SerializeProposal_ValuelessAfterValued_OmitsPreviousValue()
    {
        byte[] value = Encoding.UTF8.GetBytes("sticky");
        BaseHandler.SerializeProposal(KeyValueRequestType.TrySet, Proposal(KeyValueRequestType.TrySet, value, KeyValueState.Set), Now);

        byte[] deleteBytes = BaseHandler.SerializeProposal(KeyValueRequestType.TryDelete, Proposal(KeyValueRequestType.TryDelete, null, KeyValueState.Deleted), Now);

        byte[] expected = ReplicationSerializer.Serialize(new KeyValueMessage
        {
            Type = (int)KeyValueRequestType.TryDelete,
            Key = "space/k",
            Revision = 42,
            ExpireNode = Expires.N,
            ExpirePhysical = Expires.L,
            ExpireCounter = Expires.C,
            LastUsedNode = LastUsed.N,
            LastUsedPhysical = LastUsed.L,
            LastUsedCounter = LastUsed.C,
            LastModifiedNode = LastModified.N,
            LastModifiedPhysical = LastModified.L,
            LastModifiedCounter = LastModified.C,
            TimeNode = Now.N,
            TimePhysical = Now.L,
            TimeCounter = Now.C,
            NoRevision = false
        });

        Assert.Equal(expected, deleteBytes);
        Assert.False(ReplicationSerializer.UnserializeKeyValueMessage(deleteBytes).HasValue);
    }
}

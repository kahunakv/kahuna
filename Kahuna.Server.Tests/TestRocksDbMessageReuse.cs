
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Kahuna.Persistence.Protos;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards the reused RocksDB row-message shells and the ordering-only row decoder. Proto3 omits
/// default-valued fields from the wire and a merge into a dirty instance keeps every omitted field,
/// so a reused shell that is not fully reset would leak one row's fields into the next (a delete
/// inheriting the prior set's value). The descriptor sweep forces the fully-populated fixture to
/// cover every proto field, so a field added to the proto but missed by the reset fails these tests
/// automatically. The ordering decoder must agree with the full protobuf parse on every wire shape
/// it accepts, and must fall back (return false) instead of guessing on shapes it does not.
/// </summary>
public sealed class TestRocksDbMessageReuse
{
    /// <summary>Populates every field of the key-value row message, including the optional value.</summary>
    private static RocksDbKeyValueMessage FullyPopulatedKeyValue() => new()
    {
        Key = "space/full",
        Value = ByteString.CopyFromUtf8("payload"),
        ExpiresNode = 1,
        ExpiresPhysical = 2,
        ExpiresCounter = 3,
        LastUsedNode = 4,
        LastUsedPhysical = 5,
        LastUsedCounter = 6,
        LastModifiedNode = 7,
        LastModifiedPhysical = 8,
        LastModifiedCounter = 9,
        Revision = 42,
        State = 2
    };

    /// <summary>Populates every field of the lock row message, including the optional owner.</summary>
    private static RocksDbLockMessage FullyPopulatedLock() => new()
    {
        Resource = "lock/full",
        Owner = ByteString.CopyFromUtf8("owner"),
        ExpiresNode = 1,
        ExpiresPhysical = 2,
        ExpiresCounter = 3,
        LastUsedNode = 4,
        LastUsedPhysical = 5,
        LastUsedCounter = 6,
        LastModifiedNode = 7,
        LastModifiedPhysical = 8,
        LastModifiedCounter = 9,
        FencingToken = 12,
        State = 2
    };

    /// <summary>
    /// Every proto field must be set to a non-default value by the fully-populated fixture.
    /// A field added to the proto later fails here until the fixture — and therefore the
    /// reset-completeness tests below — covers it.
    /// </summary>
    [Fact]
    public void FullyPopulatedFixtures_CoverEveryProtoField()
    {
        RocksDbKeyValueMessage fullKeyValue = FullyPopulatedKeyValue();
        RocksDbKeyValueMessage pristineKeyValue = new();

        foreach (FieldDescriptor field in RocksDbKeyValueMessage.Descriptor.Fields.InFieldNumberOrder())
            Assert.False(
                Equals(field.Accessor.GetValue(fullKeyValue), field.Accessor.GetValue(pristineKeyValue)),
                $"FullyPopulatedKeyValue must set field '{field.Name}' to a non-default value");

        RocksDbLockMessage fullLock = FullyPopulatedLock();
        RocksDbLockMessage pristineLock = new();

        foreach (FieldDescriptor field in RocksDbLockMessage.Descriptor.Fields.InFieldNumberOrder())
            Assert.False(
                Equals(field.Accessor.GetValue(fullLock), field.Accessor.GetValue(pristineLock)),
                $"FullyPopulatedLock must set field '{field.Name}' to a non-default value");
    }

    /// <summary>
    /// A reset applied to a fully-populated message must equal a pristine message. Protobuf
    /// equality covers every field and its presence bit, so this fails for any field the reset
    /// misses.
    /// </summary>
    [Fact]
    public void ResetKeyValueMessage_AfterFullPopulation_EqualsPristineMessage()
    {
        RocksDbKeyValueMessage message = FullyPopulatedKeyValue();

        RocksDbPersistenceBackend.ResetKeyValueMessage(message);

        Assert.Equal(new RocksDbKeyValueMessage(), message);
        Assert.False(message.HasValue);
    }

    [Fact]
    public void ResetLockMessage_AfterFullPopulation_EqualsPristineMessage()
    {
        RocksDbLockMessage message = FullyPopulatedLock();

        RocksDbPersistenceBackend.ResetLockMessage(message);

        Assert.Equal(new RocksDbLockMessage(), message);
        Assert.False(message.HasOwner);
    }

    /// <summary>
    /// A sparse row merged into a shell right after a fully-populated one must match a fresh parse
    /// of the same bytes: no field of the earlier row may survive into it. This is the exact
    /// reset-then-merge sequence the scan-loop shell reuse performs per row.
    /// </summary>
    [Fact]
    public void ShellParse_SparseAfterFull_MatchesFreshParse()
    {
        byte[] sparse = new RocksDbKeyValueMessage { Revision = 7, State = 3 }.ToByteArray();

        RocksDbKeyValueMessage shell = FullyPopulatedKeyValue();
        RocksDbPersistenceBackend.ResetKeyValueMessage(shell);
        shell.MergeFrom(sparse);

        Assert.Equal(RocksDbKeyValueMessage.Parser.ParseFrom(sparse), shell);
        Assert.False(shell.HasValue);
        Assert.Equal(string.Empty, shell.Key);
    }

    /// <summary>
    /// The ordering-only decoder must agree with the full protobuf parse on realistic rows,
    /// including negative varints (10-byte encodings), the counter's full unsigned range, and
    /// rows with and without a value payload.
    /// </summary>
    [Fact]
    public void TryDecodeKeyValueOrdering_MatchesFullParse()
    {
        RocksDbKeyValueMessage[] samples =
        [
            new(),
            FullyPopulatedKeyValue(),
            new() { Revision = long.MaxValue, LastModifiedPhysical = long.MaxValue, LastModifiedCounter = uint.MaxValue, LastModifiedNode = int.MaxValue },
            new() { Revision = -1, LastModifiedPhysical = -1, LastModifiedNode = -1 },
            new() { Value = ByteString.CopyFrom(new byte[4096]), Revision = 5, LastModifiedNode = 2, LastModifiedPhysical = 1_000_000, LastModifiedCounter = 17 },
            new() { Key = "only/a/key" },
        ];

        foreach (RocksDbKeyValueMessage sample in samples)
        {
            byte[] serialized = sample.ToByteArray();
            RocksDbKeyValueMessage parsed = RocksDbKeyValueMessage.Parser.ParseFrom(serialized);

            Assert.True(RocksDbPersistenceBackend.TryDecodeKeyValueOrdering(
                serialized, out RocksDbPersistenceBackend.StoredKeyValueOrdering ordering));

            Assert.Equal(parsed.Revision, ordering.Revision);
            Assert.Equal(parsed.LastModifiedNode, ordering.LastModifiedNode);
            Assert.Equal(parsed.LastModifiedPhysical, ordering.LastModifiedPhysical);
            Assert.Equal(parsed.LastModifiedCounter, ordering.LastModifiedCounter);
        }
    }

    /// <summary>
    /// Truncated data must not decode: the fast path returns false and the fallback surfaces the
    /// same exception the full parser always raised for that input.
    /// </summary>
    [Fact]
    public void DecodeKeyValueOrdering_TruncatedData_ThrowsLikeFullParse()
    {
        byte[] serialized = FullyPopulatedKeyValue().ToByteArray();
        byte[] truncated = serialized[..(serialized.Length - 1)];

        Assert.False(RocksDbPersistenceBackend.TryDecodeKeyValueOrdering(truncated, out _));
        Assert.Throws<InvalidProtocolBufferException>(() =>
            RocksDbPersistenceBackend.DecodeKeyValueOrdering(truncated));
    }
}

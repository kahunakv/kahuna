using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// Pins the numeric value of every <see cref="KeyValueRequestType"/> member. The number is persisted as the
/// <c>Type</c> of a replicated key-value message in Raft logs, WAL segments and backups, so a renumbered member
/// makes a node read records an older node wrote as a different operation. A member once inserted in the middle
/// of the enum shifted <see cref="KeyValueRequestType.MaterializeIntent"/> from 29 to 30, and a node restarted on
/// that build skipped every by-reference commit in its replay window as an unknown type. A new member must be
/// appended with the next unused value and added to this table; an existing row must never change.
/// </summary>
public sealed class TestKeyValueRequestTypeWireValues
{
    private static readonly (KeyValueRequestType Member, int Value)[] PinnedValues =
    [
        (KeyValueRequestType.TrySet, 0),
        (KeyValueRequestType.TryExtend, 1),
        (KeyValueRequestType.TryDelete, 2),
        (KeyValueRequestType.TryGet, 3),
        (KeyValueRequestType.TryExists, 4),
        (KeyValueRequestType.TryAcquireExclusiveLock, 5),
        (KeyValueRequestType.TryAcquireExclusivePrefixLock, 6),
        (KeyValueRequestType.TryAcquireExclusiveRangeLock, 7),
        (KeyValueRequestType.TryReleaseExclusiveLock, 8),
        (KeyValueRequestType.TryReleaseExclusivePrefixLock, 9),
        (KeyValueRequestType.TryReleaseExclusiveRangeLock, 10),
        (KeyValueRequestType.TryPrepareMutations, 11),
        (KeyValueRequestType.TryCommitMutations, 12),
        (KeyValueRequestType.TryRollbackMutations, 13),
        (KeyValueRequestType.ScanByPrefix, 14),
        (KeyValueRequestType.ScanByPrefixFromDisk, 15),
        (KeyValueRequestType.GetByBucket, 16),
        (KeyValueRequestType.GetByRange, 17),
        (KeyValueRequestType.CompleteProposal, 18),
        (KeyValueRequestType.ReleaseProposal, 19),
        (KeyValueRequestType.Collect, 20),
        (KeyValueRequestType.TryCheckWriteIntent, 21),
        (KeyValueRequestType.GetRangeLocks, 22),
        (KeyValueRequestType.ImportRangeLocks, 23),
        (KeyValueRequestType.GetSafeTimestamp, 24),
        (KeyValueRequestType.ResumeRead, 25),
        (KeyValueRequestType.InvalidateOrApply, 26),
        (KeyValueRequestType.FlushAck, 27),
        (KeyValueRequestType.EvictPartition, 28),
        (KeyValueRequestType.MaterializeIntent, 29),
        (KeyValueRequestType.DropLeaderState, 30),
        (KeyValueRequestType.TryFinalizeMutation, 31),
        (KeyValueRequestType.RunActorTurn, 32),
    ];

    [Fact]
    public void EveryPinnedMemberKeepsItsValue()
    {
        foreach ((KeyValueRequestType member, int value) in PinnedValues)
            Assert.True((int)member == value, $"{member} is {(int)member}; persisted records expect {value}");
    }

    [Fact]
    public void PersistedValuesDecodeToTheSameMember()
    {
        // The replay path casts the stored integer back to the enum: the value an older build wrote must
        // name the same operation on this build.
        foreach ((KeyValueRequestType member, int value) in PinnedValues)
            Assert.Equal(member, (KeyValueRequestType)value);
    }

    [Fact]
    public void EveryMemberIsPinnedAndNoValueIsShared()
    {
        KeyValueRequestType[] members = Enum.GetValues<KeyValueRequestType>();

        HashSet<KeyValueRequestType> pinned = new(PinnedValues.Length);
        foreach ((KeyValueRequestType member, _) in PinnedValues)
            pinned.Add(member);

        foreach (KeyValueRequestType member in members)
            Assert.True(pinned.Contains(member), $"{member} ({(int)member}) is not pinned; append it to the table with its value");

        // Enum.GetValues returns one entry per name, so fewer distinct values than names means two names share a value.
        Assert.Equal(Enum.GetNames<KeyValueRequestType>().Length, members.Distinct().Count());
    }
}

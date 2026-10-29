using Kahuna.Shared.Routing;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Chooses the data partition a key-range space's first, whole-space descriptor starts on. A pure
/// function of the space, the committed descriptor set and the hash pool, so the choice is the same
/// on every node and can be tested without a cluster.
///
/// <para>
/// <b>A new placement group seeds where hash routing places it.</b> The seed is the partition
/// <see cref="DataPartitionRouter.HashPartitionOfKeySpace"/> gives the space, so a space starts on the
/// same partition in both routing modes: a group that is flipped to key-range routing lands where its
/// hash-routed rows already are, and the key-range placement of a set of groups is exactly the hash
/// placement of the same groups. An earlier seed used a different digest over the same pool, which
/// piled groups that hash routing spreads over three partitions onto one.
/// </para>
///
/// <para>
/// <b>A new space of a seeded group follows its group.</b> When a space of the same placement group
/// (<see cref="HashPlacement.GroupOf(string)"/>) already has a descriptor, the new space seeds on
/// that group's partition rather than on the hash bucket. The bucket alone would separate a table's
/// late-created index space from its rows whenever the rows were seeded by the earlier digest, or by
/// an older binary that leads the meta partition during a rolling upgrade; a separated table loses the
/// one-phase commit with no automatic repair. The rule for a group whose spaces sit on several
/// partitions after splits or moves is fixed so that every node derives one answer:
/// <list type="number">
/// <item>the anchor space is the sibling named exactly like the group (a space with no group
/// separator: the row space in a rows-plus-indexes layout) when it is seeded, otherwise the lowest
/// sibling key space in ordinal order;</item>
/// <item>the anchor partition is that of the anchor space's head descriptor, the one whose
/// <see cref="RangeDescriptor.StartKey"/> is null. Every seeded space has exactly one.</item>
/// </list>
/// Existing descriptors never change and no data moves: the rule decides only where a space that is
/// seeded from now on starts.
/// </para>
/// </summary>
internal static class KeyRangeSeedPlacement
{
    /// <summary>
    /// The partition <paramref name="keySpace"/>'s whole-space descriptor seeds on, given the
    /// descriptor set <paramref name="descriptors"/> the seed is applied to and a hash pool of
    /// <paramref name="poolSize"/> partitions.
    /// </summary>
    internal static int SeedPartitionFor(string keySpace, IReadOnlyList<RangeDescriptor> descriptors, int poolSize)
    {
        if (TryFindGroupAnchor(keySpace, descriptors, out int anchorPartition))
            return anchorPartition;

        return DataPartitionRouter.HashPartitionOfKeySpace(keySpace, poolSize);
    }

    /// <summary>
    /// Finds the partition of the anchor space of <paramref name="keySpace"/>'s placement group among
    /// <paramref name="descriptors"/>. False when no other space of the group is seeded.
    /// </summary>
    internal static bool TryFindGroupAnchor(string keySpace, IReadOnlyList<RangeDescriptor> descriptors, out int partitionId)
    {
        ReadOnlySpan<char> group = HashPlacement.GroupOf(keySpace.AsSpan());

        string? anchorSpace = null;
        partitionId = 0;

        for (int i = 0; i < descriptors.Count; i++)
        {
            RangeDescriptor descriptor = descriptors[i];

            // Only a head descriptor represents its space: a split space has one descriptor per range,
            // and the head is the one every node agrees on.
            if (descriptor.StartKey is not null)
                continue;

            string sibling = descriptor.KeySpace;

            if (string.Equals(sibling, keySpace, StringComparison.Ordinal))
                continue;

            if (!HashPlacement.GroupOf(sibling.AsSpan()).SequenceEqual(group))
                continue;

            if (anchorSpace is not null && !IsPreferredAnchor(sibling, anchorSpace, group))
                continue;

            anchorSpace = sibling;
            partitionId = descriptor.PartitionId;
        }

        return anchorSpace is not null;
    }

    /// <summary>
    /// True when <paramref name="candidate"/> is a better anchor than <paramref name="current"/>: the
    /// space named exactly like the group beats every other, and otherwise the lower ordinal name wins.
    /// </summary>
    private static bool IsPreferredAnchor(string candidate, string current, ReadOnlySpan<char> group)
    {
        bool candidateIsGroup = candidate.AsSpan().SequenceEqual(group);
        bool currentIsGroup = current.AsSpan().SequenceEqual(group);

        if (candidateIsGroup != currentIsGroup)
            return candidateIsGroup;

        return string.CompareOrdinal(candidate, current) < 0;
    }
}

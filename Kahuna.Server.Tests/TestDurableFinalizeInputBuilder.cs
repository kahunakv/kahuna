using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="DurableFinalizeInputBuilder"/> — the freeze that turns a transaction's modified keys
/// and staged values into a <see cref="DurableFinalizeInput"/>: all-persistent gating, anchor + manifest,
/// per-partition grouping, delete-vs-set state derivation, the lossless-or-fall-back guard, and the dispatch
/// fences each partition group carries (one key per descriptor, at that key's own generation).
/// </summary>
public sealed class TestDurableFinalizeInputBuilder
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static readonly HLCTimestamp TxId = Ts(1000);
    private const long Epoch = 1;
    private static readonly HLCTimestamp CommitTs = Ts(1100);
    private static readonly HLCTimestamp Deadline = Ts(31100);

    // Deterministic key→partition mapping for the tests: every key resolves at generation 7 through the
    // whole-space descriptor of its own space.
    private static DurableKeyRoute Locate(string key) => new(key.StartsWith("idx/", StringComparison.Ordinal) ? 8 : 5, 7L, RangeStartKey: null);

    private static bool Build(
        (string, KeyValueDurability)[] modifiedKeys,
        Dictionary<string, StagedValue> staged,
        string anchor,
        Func<string, DurableKeyRoute> locate,
        out DurableFinalizeInput? input) =>
        DurableFinalizeInputBuilder.TryBuild(TxId, Epoch, "coord", anchor, CommitTs, Deadline, modifiedKeys, staged, locate, out input);

    private static bool Build(
        (string, KeyValueDurability)[] modifiedKeys,
        Dictionary<string, StagedValue> staged,
        string anchor,
        out DurableFinalizeInput? input) =>
        DurableFinalizeInputBuilder.TryBuild(TxId, Epoch, "coord", anchor, CommitTs, Deadline, modifiedKeys, staged, Locate, out input);

    private static StagedValue Set(long revision, byte[] value) => new(value, KeyValueState.Set, revision, ExpiresMs: 0, NoRevision: false);

    private static StagedValue SetNoRev(long revision, byte[] value) => new(value, KeyValueState.Set, revision, ExpiresMs: 0, NoRevision: true);
    private static StagedValue Delete(long revision) => new(null, KeyValueState.Deleted, revision, ExpiresMs: 0, NoRevision: false);

    [Fact]
    public void SinglePersistentKey_Builds()
    {
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(3, [9]) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.True(ok);
        Assert.NotNull(input);
        Assert.Equal("acct/1", input!.RecordAnchorKey);
        Assert.Equal(5, input.AnchorPartitionId);
        Assert.Equal(7L, input.AnchorGeneration); // the descriptor generation captured at freeze, for the dispatch re-fence
        Assert.Single(input.Partitions);
        // The one key is the group's only fence, at the generation it resolved to.
        Assert.Equal(new RangeFence("acct/1", 7L), Assert.Single(input.Partitions[0].Fences));
        Assert.Equal(KeyValueState.Set, input.Partitions[0].Intents[0].State);
        Assert.Equal(3, input.Partitions[0].Intents[0].Revision);
    }

    [Fact]
    public void MultiKey_GroupsByPartition()
    {
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent), ("idx/name/bob", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(1, [1]), ["idx/name/bob"] = Set(1, [2]) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.True(ok);
        Assert.Equal(2, input!.Partitions.Count);
        Assert.Contains(input.Partitions, p => p.PartitionId == 5);
        Assert.Contains(input.Partitions, p => p.PartitionId == 8);
        Assert.Equal(2, input.Manifest.Count);
    }

    [Fact]
    public void MixedDurability_FallsBack()
    {
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent), ("cache/x", KeyValueDurability.Ephemeral)],
            new() { ["acct/1"] = Set(1, [1]), ["cache/x"] = Set(1, [2]) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.False(ok);
        Assert.Null(input);
    }

    [Fact]
    public void MissingStagedValue_FallsBack()
    {
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent), ("acct/2", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(1, [1]) }, // acct/2 has no staged value
            "acct/1", out DurableFinalizeInput? input);

        Assert.False(ok);
        Assert.Null(input);
    }

    [Fact]
    public void NoAnchor_FallsBack()
    {
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(1, [1]) },
            anchor: "", out DurableFinalizeInput? input);

        Assert.False(ok);
        Assert.Null(input);
    }

    [Fact]
    public void EmptyModifiedKeys_FallsBack()
    {
        bool ok = Build([], new(), "acct/1", out DurableFinalizeInput? input);
        Assert.False(ok);
        Assert.Null(input);
    }

    [Fact]
    public void Deletion_NullValue_IsDeletedState()
    {
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Delete(4) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.True(ok);
        Assert.Equal(KeyValueState.Deleted, input!.Partitions[0].Intents[0].State);
        Assert.Null(input.Partitions[0].Intents[0].Value);
    }

    [Fact]
    public void ValuelessSet_StaysSetState_NotDelete()
    {
        // A set staged with a null value is a key that exists and holds nothing. The staged state — not value
        // presence — decides the intent, so the transaction commits the key as Set, exactly as a direct
        // null-value set and the ticket path do.
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = new StagedValue(null, KeyValueState.Set, 3, ExpiresMs: 0, NoRevision: false) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.True(ok);
        Assert.Equal(KeyValueState.Set, input!.Partitions[0].Intents[0].State);
        Assert.Null(input.Partitions[0].Intents[0].Value);
    }

    [Fact]
    public void UndefinedStagedState_FallsBack()
    {
        // Every staging site records Set or Deleted; an Undefined state means the entry is not lossless, so
        // the freeze refuses it and the transaction takes the ticket path.
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = new StagedValue([9], KeyValueState.Undefined, 3, ExpiresMs: 0, NoRevision: false) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.False(ok);
        Assert.Null(input);
    }

    [Fact]
    public void TtlSet_ResolvesRelativeExpiryToCommitTimestampPlusMs()
    {
        // A staged relative TTL of 5000ms freezes to an absolute expiry anchored to the commit timestamp, not a
        // wall clock — so a TTL write is now durable-atomic instead of falling back to the ticket path.
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = new StagedValue([9], KeyValueState.Set, 3, ExpiresMs: 5000, NoRevision: false) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.True(ok);
        Assert.Equal(new HLCTimestamp(CommitTs.N, CommitTs.L + 5000, CommitTs.C), input!.Partitions[0].Intents[0].Expires);
    }

    [Fact]
    public void NoRevisionSet_PreservesFlagOnIntent()
    {
        // A staged SET NOREV freezes with NoRevision set, so the materialized durable write suppresses history
        // exactly as a direct write would — the flag is no longer dropped at staging.
        Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = SetNoRev(3, [9]) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.True(input!.Partitions[0].Intents[0].NoRevision);
    }

    [Fact]
    public void OrdinarySet_DoesNotSuppressRevision()
    {
        Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(3, [9]) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.False(input!.Partitions[0].Intents[0].NoRevision);
    }

    [Fact]
    public void Intent_CarriesBucketDerivedFromKey()
    {
        // The prepared intent's bucket is the key's parent prefix, matching what the actor apply path recomputes,
        // rather than the null placeholder the freeze previously fabricated. A top-level key (no '/') has none.
        Build(
            [("acct/1", KeyValueDurability.Persistent), ("root", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(1, [1]), ["root"] = Set(1, [2]) },
            "acct/1", out DurableFinalizeInput? input);

        PreparedIntent acct = input!.Partitions.SelectMany(p => p.Intents).Single(i => i.Key == "acct/1");
        PreparedIntent root = input.Partitions.SelectMany(p => p.Intents).Single(i => i.Key == "root");
        Assert.Equal("acct", acct.Bucket);
        Assert.Null(root.Bucket);
    }

    [Fact]
    public void NonTtlSet_HasNoExpiry()
    {
        Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(3, [9]) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.Equal(HLCTimestamp.Zero, input!.Partitions[0].Intents[0].Expires);
    }

    [Fact]
    public void AllIntents_ShareOneCommitTimestampAndManifestHash()
    {
        Build(
            [("acct/1", KeyValueDurability.Persistent), ("idx/name/bob", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = Set(1, [1]), ["idx/name/bob"] = Set(1, [2]) },
            "acct/1", out DurableFinalizeInput? input);

        long expectedHash = TransactionManifest.ComputeHash(TxId, Epoch, "acct/1", CommitTs, input!.Manifest);
        foreach (DurablePartitionPrepare partition in input.Partitions)
            foreach (PreparedIntent intent in partition.Intents)
            {
                Assert.Equal(CommitTs, intent.CommitTimestamp);
                Assert.Equal(expectedHash, intent.ManifestHash);
            }

        Assert.Equal(expectedHash, input.ManifestHash);
    }

    // ── Dispatch fences ─────────────────────────────────────────────────────────────────────────────────────

    private static Func<string, DurableKeyRoute> Routes(Dictionary<string, DurableKeyRoute> byKey) =>
        key => byKey.TryGetValue(key, out DurableKeyRoute route) ? route : throw new InvalidOperationException($"no route for {key}");

    private static (string, KeyValueDurability)[] Persistent(params string[] keys) =>
        keys.Select(k => (k, KeyValueDurability.Persistent)).ToArray();

    private static Dictionary<string, StagedValue> StagedSets(params string[] keys) =>
        keys.ToDictionary(k => k, _ => Set(1, [1]), StringComparer.Ordinal);

    [Fact]
    public void MixedSpacesOnOnePartition_FenceEachSpaceWithItsOwnKeyAndGeneration()
    {
        // A key-range row space (generation 1) and a hash-routed meta space (generation 0) share partition 3,
        // with the key-range key first. The group must carry one fence per space, each pairing the key with the
        // generation that key resolved to: pairing the first key with the last key's generation read as a moved
        // range at every dispatch and refused the commit on every round.
        Dictionary<string, DurableKeyRoute> routes = new(StringComparer.Ordinal)
        {
            ["rows/1"] = new(3, 1L, RangeStartKey: null),
            ["db/meta/t"] = new(3, 0L, RangeStartKey: null)
        };

        Assert.True(Build(Persistent("rows/1", "db/meta/t"), StagedSets("rows/1", "db/meta/t"), "rows/1", Routes(routes), out DurableFinalizeInput? input));

        DurablePartitionPrepare group = Assert.Single(input!.Partitions);
        Assert.Equal(3, group.PartitionId);
        Assert.Equal(2, group.Fences.Count);

        foreach (RangeFence fence in group.Fences)
            Assert.Equal(routes[fence.Key].Generation, fence.Generation);

        Assert.Contains(new RangeFence("rows/1", 1L), group.Fences);
        Assert.Contains(new RangeFence("db/meta/t", 0L), group.Fences);
    }

    [Fact]
    public void KeyOrderDoesNotChangeTheFences()
    {
        // The modified-key order comes from a hash set in production; whichever key the freeze meets first, the
        // fences are the same pairs.
        Dictionary<string, DurableKeyRoute> routes = new(StringComparer.Ordinal)
        {
            ["rows/1"] = new(3, 1L, RangeStartKey: null),
            ["db/meta/t"] = new(3, 0L, RangeStartKey: null)
        };

        Assert.True(Build(Persistent("rows/1", "db/meta/t"), StagedSets("rows/1", "db/meta/t"), "rows/1", Routes(routes), out DurableFinalizeInput? rowsFirst));
        Assert.True(Build(Persistent("db/meta/t", "rows/1"), StagedSets("rows/1", "db/meta/t"), "rows/1", Routes(routes), out DurableFinalizeInput? metaFirst));

        Assert.Equal(
            rowsFirst!.Partitions[0].Fences.OrderBy(f => f.Key, StringComparer.Ordinal),
            metaFirst!.Partitions[0].Fences.OrderBy(f => f.Key, StringComparer.Ordinal));
    }

    [Fact]
    public void TwoKeyRangeSpacesAtDifferentGenerations_FenceBoth()
    {
        // Two key-range spaces on one partition, at generations 1 and 2 (only one of them split so far).
        Dictionary<string, DurableKeyRoute> routes = new(StringComparer.Ordinal)
        {
            ["t:r/1"] = new(2, 1L, RangeStartKey: null),
            ["t:i:5/abc"] = new(2, 2L, RangeStartKey: null)
        };

        Assert.True(Build(Persistent("t:r/1", "t:i:5/abc"), StagedSets("t:r/1", "t:i:5/abc"), "t:r/1", Routes(routes), out DurableFinalizeInput? input));

        DurablePartitionPrepare group = Assert.Single(input!.Partitions);
        Assert.Contains(new RangeFence("t:r/1", 1L), group.Fences);
        Assert.Contains(new RangeFence("t:i:5/abc", 2L), group.Fences);
        Assert.Equal(2, group.Fences.Count);
    }

    [Fact]
    public void TwoDescriptorsOfOneSpace_FenceEachDescriptor()
    {
        // The space split: keys below "m" and keys from "m" live on two descriptors of the same partition at
        // different generations. A fence per descriptor sees a later move of either half.
        Dictionary<string, DurableKeyRoute> routes = new(StringComparer.Ordinal)
        {
            ["rows/a"] = new(4, 2L, RangeStartKey: null),
            ["rows/b"] = new(4, 2L, RangeStartKey: null),
            ["rows/m"] = new(4, 5L, RangeStartKey: "rows/m"),
            ["rows/z"] = new(4, 5L, RangeStartKey: "rows/m")
        };

        Assert.True(Build(Persistent("rows/a", "rows/b", "rows/m", "rows/z"), StagedSets("rows/a", "rows/b", "rows/m", "rows/z"), "rows/a", Routes(routes), out DurableFinalizeInput? input));

        DurablePartitionPrepare group = Assert.Single(input!.Partitions);
        Assert.Equal(2, group.Fences.Count);
        Assert.Contains(new RangeFence("rows/a", 2L), group.Fences);
        Assert.Contains(new RangeFence("rows/m", 5L), group.Fences);
    }

    [Fact]
    public void KeysOfOneDescriptor_ShareOneFence()
    {
        Assert.True(Build(Persistent("acct/1", "acct/2", "acct/3"), StagedSets("acct/1", "acct/2", "acct/3"), "acct/1", Locate, out DurableFinalizeInput? input));

        DurablePartitionPrepare group = Assert.Single(input!.Partitions);
        Assert.Equal(new RangeFence("acct/1", 7L), Assert.Single(group.Fences));
    }

    [Fact]
    public void AnchorFence_RidesOnTheAnchorPartitionGroup()
    {
        // The anchor key is not among the written keys but routes to the partition that holds them: the group's
        // bundle carries the record initialization, so it is fenced against the anchor descriptor as well.
        Dictionary<string, DurableKeyRoute> routes = new(StringComparer.Ordinal)
        {
            ["rows/1"] = new(5, 1L, RangeStartKey: null),
            ["coord/tx-1"] = new(5, 4L, RangeStartKey: null)
        };

        Assert.True(Build(Persistent("rows/1"), StagedSets("rows/1"), "coord/tx-1", Routes(routes), out DurableFinalizeInput? input));

        Assert.Equal(5, input!.AnchorPartitionId);
        Assert.Equal(4L, input.AnchorGeneration);
        Assert.Equal(new RangeFence("coord/tx-1", 4L), Assert.Single(input.AnchorFence));

        DurablePartitionPrepare group = Assert.Single(input.Partitions);
        Assert.Equal(2, group.Fences.Count);
        Assert.Contains(new RangeFence("rows/1", 1L), group.Fences);
        Assert.Contains(new RangeFence("coord/tx-1", 4L), group.Fences);
    }

    [Fact]
    public void AnchorOutsideTheParticipants_IsFencedByTheRecordPathAlone()
    {
        Dictionary<string, DurableKeyRoute> routes = new(StringComparer.Ordinal)
        {
            ["rows/1"] = new(5, 1L, RangeStartKey: null),
            ["coord/tx-1"] = new(9, 4L, RangeStartKey: null)
        };

        Assert.True(Build(Persistent("rows/1"), StagedSets("rows/1"), "coord/tx-1", Routes(routes), out DurableFinalizeInput? input));

        Assert.Equal(9, input!.AnchorPartitionId);
        DurablePartitionPrepare group = Assert.Single(input.Partitions);
        Assert.Equal(5, group.PartitionId);
        Assert.Equal(new RangeFence("rows/1", 1L), Assert.Single(group.Fences));
        Assert.Equal(new RangeFence("coord/tx-1", 4L), Assert.Single(input.AnchorFence));
    }
}

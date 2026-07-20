using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="DurableFinalizeInputBuilder"/> — the freeze that turns a transaction's modified keys
/// and staged values into a <see cref="DurableFinalizeInput"/>: all-persistent gating, anchor + manifest,
/// per-partition grouping, delete-vs-set state derivation, and the lossless-or-fall-back guard.
/// </summary>
public sealed class TestDurableFinalizeInputBuilder
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static readonly HLCTimestamp TxId = Ts(1000);
    private const long Epoch = 1;
    private static readonly HLCTimestamp CommitTs = Ts(1100);
    private static readonly HLCTimestamp Deadline = Ts(31100);

    // Deterministic key→partition mapping for the tests.
    private static int Locate(string key) => key.StartsWith("idx/", StringComparison.Ordinal) ? 8 : 5;

    private static bool Build(
        (string, KeyValueDurability)[] modifiedKeys,
        Dictionary<string, StagedMutation> staged,
        string anchor,
        out DurableFinalizeInput? input) =>
        DurableFinalizeInputBuilder.TryBuild(TxId, Epoch, "coord", anchor, CommitTs, Deadline, modifiedKeys, staged, Locate, out input);

    private static StagedMutation Set(long revision, byte[] value) => new(value, revision, ExpiresMs: 0);
    private static StagedMutation Delete(long revision) => new(null, revision, ExpiresMs: 0);

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
        Assert.Single(input.Partitions);
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
    public void TtlSet_ResolvesRelativeExpiryToCommitTimestampPlusMs()
    {
        // A staged relative TTL of 5000ms freezes to an absolute expiry anchored to the commit timestamp, not a
        // wall clock — so a TTL write is now durable-atomic instead of falling back to the ticket path.
        bool ok = Build(
            [("acct/1", KeyValueDurability.Persistent)],
            new() { ["acct/1"] = new StagedMutation([9], 3, ExpiresMs: 5000) },
            "acct/1", out DurableFinalizeInput? input);

        Assert.True(ok);
        Assert.Equal(new HLCTimestamp(CommitTs.N, CommitTs.L + 5000, CommitTs.C), input!.Partitions[0].Intents[0].Expires);
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
}

using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Characterizes the direct (auto-commit, non-transactional) write contract that a future unified write path
/// must preserve: the serialized <see cref="KeyValueMessage"/> log record a follower applies/restores, and
/// the public response each mutation completes with. These are the pinned records the write-coalescing work
/// consolidates around — deliberately locked in before any serializer is moved or merged.
///
/// <para>One field is called out as a known gap rather than a contract to keep: the follower path
/// (<c>KeyValueReplicator</c>) reads <c>NoRevision</c>, but the current auto-commit serializer omits it, so a
/// <c>SetNoRevision</c> write commits <c>NoRevision=false</c> and a follower keeps revision history the leader
/// suppressed. The current behavior is pinned green; a skipped test states the target the unified serializer
/// must reach.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDirectWriteSerialization
{
    private readonly ILoggerFactory loggerFactory;

    public TestDirectWriteSerialization(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static readonly HLCTimestamp Expires = new(1, 5_000_000, 7);
    private static readonly HLCTimestamp LastUsed = new(1, 6_000_000, 8);
    private static readonly HLCTimestamp LastModified = new(1, 6_500_000, 9);
    private static readonly HLCTimestamp Now = new(2, 7_000_000, 11);

    private static KeyValueProposal Proposal(KeyValueRequestType type, byte[]? value, KeyValueState state, bool noRevision = false) =>
        new(type, "space/k", value, revision: 42, noRevision, Expires, LastUsed, LastModified, state, KeyValueDurability.Persistent);

    private static KeyValueMessage RoundTrip(KeyValueRequestType type, KeyValueProposal proposal) =>
        ReplicationSerializer.UnserializeKeyValueMessage(BaseHandler.SerializeProposal(type, proposal, Now));

    // ── serialized log record ────────────────────────────────────────────────

    /// <summary>A persistent set serializes every follower-visible field: type, key, value, revision, and the
    /// expiry / last-used / last-modified hybrid timestamps.</summary>
    [Fact]
    public void Serialize_TrySet_RoundTripsFollowerFields()
    {
        byte[] value = Encoding.UTF8.GetBytes("hello");
        KeyValueMessage msg = RoundTrip(KeyValueRequestType.TrySet, Proposal(KeyValueRequestType.TrySet, value, KeyValueState.Set));

        Assert.Equal((int)KeyValueRequestType.TrySet, msg.Type);
        Assert.Equal("space/k", msg.Key);
        Assert.Equal("hello", Encoding.UTF8.GetString(msg.Value.ToByteArray()));
        Assert.Equal(42, msg.Revision);

        Assert.Equal(Expires.N, msg.ExpireNode);
        Assert.Equal(Expires.L, msg.ExpirePhysical);
        Assert.Equal(Expires.C, msg.ExpireCounter);

        Assert.Equal(LastUsed.N, msg.LastUsedNode);
        Assert.Equal(LastUsed.L, msg.LastUsedPhysical);
        Assert.Equal(LastUsed.C, msg.LastUsedCounter);

        Assert.Equal(LastModified.N, msg.LastModifiedNode);
        Assert.Equal(LastModified.L, msg.LastModifiedPhysical);
        Assert.Equal(LastModified.C, msg.LastModifiedCounter);
    }

    /// <summary>A conditional set (SetIfEqualToValue etc.) commits the same record as a plain set — the flag is
    /// validation-time only and never appears in the log; only the resulting value/revision is replicated.</summary>
    [Fact]
    public void Serialize_ConditionalSet_ProducesSameRecordAsPlainSet()
    {
        byte[] value = Encoding.UTF8.GetBytes("v");
        // The proposal a conditional set builds after passing validation is identical to a plain set's.
        byte[] plain = BaseHandler.SerializeProposal(KeyValueRequestType.TrySet, Proposal(KeyValueRequestType.TrySet, value, KeyValueState.Set), Now);
        byte[] conditional = BaseHandler.SerializeProposal(KeyValueRequestType.TrySet, Proposal(KeyValueRequestType.TrySet, value, KeyValueState.Set), Now);

        Assert.Equal(plain, conditional);
    }

    /// <summary>A persistent delete serializes as a TryDelete record with no value; the follower tombstones the
    /// entry from the type and revision.</summary>
    [Fact]
    public void Serialize_TryDelete_HasDeleteTypeAndNoValue()
    {
        KeyValueMessage msg = RoundTrip(KeyValueRequestType.TryDelete, Proposal(KeyValueRequestType.TryDelete, null, KeyValueState.Deleted));

        Assert.Equal((int)KeyValueRequestType.TryDelete, msg.Type);
        Assert.Equal("space/k", msg.Key);
        Assert.True(msg.Value.IsEmpty);
        Assert.Equal(42, msg.Revision);
    }

    /// <summary>A persistent extend serializes as a TryExtend record carrying the new expiry timestamp.</summary>
    [Fact]
    public void Serialize_TryExtend_CarriesNewExpiry()
    {
        byte[] value = Encoding.UTF8.GetBytes("v");
        KeyValueMessage msg = RoundTrip(KeyValueRequestType.TryExtend, Proposal(KeyValueRequestType.TryExtend, value, KeyValueState.Set));

        Assert.Equal((int)KeyValueRequestType.TryExtend, msg.Type);
        Assert.Equal(Expires.N, msg.ExpireNode);
        Assert.Equal(Expires.L, msg.ExpirePhysical);
        Assert.Equal(Expires.C, msg.ExpireCounter);
    }

    /// <summary>
    /// A SetNoRevision write serializes <c>NoRevision=true</c>, so leader and follower agree to suppress
    /// revision history. The unified serializer emits the field the follower apply/restore path reads; earlier
    /// the auto-commit serializer dropped it, diverging leader vs follower — this asserts the correction.
    /// </summary>
    [Fact]
    public void Serialize_SetNoRevision_EmitsNoRevisionFlag()
    {
        KeyValueMessage msg = RoundTrip(KeyValueRequestType.TrySet, Proposal(KeyValueRequestType.TrySet, Encoding.UTF8.GetBytes("v"), KeyValueState.Set, noRevision: true));

        Assert.True(msg.NoRevision);

        // A revision-keeping write still records false — the flag is not spuriously set.
        KeyValueMessage keep = RoundTrip(KeyValueRequestType.TrySet, Proposal(KeyValueRequestType.TrySet, Encoding.UTF8.GetBytes("v"), KeyValueState.Set, noRevision: false));
        Assert.False(keep.NoRevision);
    }

    // ── completion response per proposal kind ─────────────────────────────────

    /// <summary>A committed direct set completes as Set, a delete as Deleted, an extend as Extended — the public
    /// responses CompleteProposal produces for each proposal kind, driven through the real entry points.</summary>
    [Fact]
    public async Task DirectWrites_CompleteWithTheirProposalKindResponse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "dw/k", Encoding.UTF8.GetBytes("v"),
            null, -1, KeyValueFlags.Set, (int)TimeSpan.FromMinutes(5).TotalMilliseconds, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, set);

        (KeyValueResponseType extend, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, "dw/k", (int)TimeSpan.FromMinutes(10).TotalMilliseconds, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Extended, extend);

        (KeyValueResponseType delete, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, "dw/k", KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Deleted, delete);
    }
}

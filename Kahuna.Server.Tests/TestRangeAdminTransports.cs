using Grpc.Core;
using Kahuna.Client;
using Kahuna.Communication.External.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Cross-transport tests for the range-administration surface.
/// <para>
/// Two transports answering the same question must answer it identically, and the way that usually
/// breaks is not a wrong value but a lost distinction — a null range bound arriving as an empty
/// string, a refusal flattened into an exception, a determinacy flag dropped on one leg. So the gRPC
/// responses here are compared field-by-field against the manager projection the REST handlers
/// serialize verbatim, rather than each transport being asserted against its own expectations.
/// </para>
/// <para>
/// Only outcomes that are repeatable are compared, so both legs observe the same cluster state: a
/// read, the idempotent second registration, and refusals that change nothing.
/// </para>
/// </summary>
public sealed class TestRangeAdminTransports
{
    private readonly ILoggerFactory loggerFactory;

    public TestRangeAdminTransports(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    private static void AssertSameRangeMap(KahunaRangeMapResponse expected, GrpcGetRangesResponse actual)
    {
        Assert.Equal(expected.Initialized, actual.Initialized);
        Assert.Equal(expected.LocalEndpoint, actual.LocalEndpoint);
        Assert.Equal(expected.KeySpaces.Count, actual.KeySpaces.Count);

        for (int i = 0; i < expected.KeySpaces.Count; i++)
        {
            KahunaKeySpaceRangesResponse expectedSpace = expected.KeySpaces[i];
            GrpcKeySpaceRanges actualSpace = actual.KeySpaces[i];

            Assert.Equal(expectedSpace.KeySpace, actualSpace.KeySpace);
            Assert.Equal(expectedSpace.RoutingMode, actualSpace.RoutingMode);
            Assert.Equal(expectedSpace.Descriptors.Count, actualSpace.Descriptors.Count);

            for (int j = 0; j < expectedSpace.Descriptors.Count; j++)
            {
                KahunaRangeDescriptorResponse expectedRange = expectedSpace.Descriptors[j];
                GrpcRangeDescriptor actualRange = actualSpace.Descriptors[j];

                // ±infinity travels as field absence. Asserting presence and value separately is the
                // point: a bound that arrives as "" instead of absent still compares equal on value
                // and would silently turn an open-ended range into one bounded by the empty string.
                Assert.Equal(expectedRange.StartKey is not null, actualRange.HasStartKey);
                Assert.Equal(expectedRange.EndKey is not null, actualRange.HasEndKey);
                Assert.Equal(expectedRange.StartKey, actualRange.HasStartKey ? actualRange.StartKey : null);
                Assert.Equal(expectedRange.EndKey, actualRange.HasEndKey ? actualRange.EndKey : null);
                Assert.Equal(expectedRange.PartitionId, actualRange.PartitionId);
                Assert.Equal(expectedRange.Generation, actualRange.Generation);
            }
        }
    }

    /// <summary>
    /// Every range-admin operation, over gRPC, compared against the projection the REST surface
    /// serializes. Both legs reach the same manager methods, so a divergence here means a mapping
    /// lost something on the way to the wire.
    /// </summary>
    [Fact]
    public async Task GrpcAndRestSurfaces_ReportTheSameValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);

        IKahuna kahuna = node.Kahuna;
        ClusterService cluster = new(node.Raft, kahuna);
        KeyValuesService keyValues = new(kahuna, NullLogger<IKahuna>.Instance);

        const string space = "transport/parity";

        // Seed first, so the comparisons below run against a space that exists. The second
        // registration is the repeatable one: it reports AlreadySeeded no matter how often it runs.
        Assert.True((await kahuna.RegisterKeyRangeWithOutcomeAsync(space, ct)).Success);

        KahunaRegisterKeyRangeResponse restRegister = await kahuna.RegisterKeyRangeWithOutcomeAsync(space, ct);
        GrpcRegisterKeyRangeResponse grpcRegister = await keyValues.RegisterKeyRange(
            new GrpcRegisterKeyRangeRequest { KeySpace = space }, new StubServerCallContext());

        Assert.Equal("AlreadySeeded", restRegister.Status);
        Assert.Equal(restRegister.Success, grpcRegister.Success);
        Assert.Equal(restRegister.Status, grpcRegister.Status);
        Assert.Equal(restRegister.Seeded, grpcRegister.Seeded);
        Assert.Equal(restRegister.RoutingMode, grpcRegister.RoutingMode);
        Assert.Equal(restRegister.DescriptorCount, grpcRegister.DescriptorCount);
        Assert.Equal(restRegister.Reason ?? "", grpcRegister.Reason);

        // Read the map both ways, unfiltered and filtered.
        AssertSameRangeMap(
            kahuna.GetRangeMap(),
            await cluster.GetRanges(new GrpcGetRangesRequest(), new StubServerCallContext()));

        AssertSameRangeMap(
            kahuna.GetRangeMap(space),
            await cluster.GetRanges(new GrpcGetRangesRequest { KeySpace = space }, new StubServerCallContext()));

        // A refusal must cross the wire as a refusal, not as an RpcException — a caller that has to
        // catch to learn "you sent a bad key space" cannot tell it from a transport failure.
        KahunaSplitRangeResponse restSplit = await kahuna.SplitRangeAtKeyWithOutcomeAsync("db/meta", "db/meta/k", ct);
        GrpcSplitRangeResponse grpcSplit = await cluster.SplitRange(
            new GrpcSplitRangeRequest { KeySpace = "db/meta", SplitKey = "db/meta/k" }, new StubServerCallContext());

        Assert.Equal("InvalidInput", restSplit.Status);
        Assert.Equal(restSplit.Success, grpcSplit.Success);
        Assert.Equal(restSplit.Status, grpcSplit.Status);
        Assert.Equal(restSplit.Determinate, grpcSplit.Determinate);
        Assert.Equal(restSplit.NewPartitionId, grpcSplit.NewPartitionId);
        Assert.Equal(restSplit.NewGeneration, grpcSplit.NewGeneration);
        Assert.Equal(restSplit.LeaderHint ?? "", grpcSplit.LeaderHint);
        Assert.Equal(restSplit.Reason ?? "", grpcSplit.Reason);

        // Nothing is eligible on a single whole-space range, so the pass is repeatable.
        KahunaMergeRangesResponse restMerge = await kahuna.MergeRangesWithOutcomeAsync(ct);
        GrpcMergeRangesResponse grpcMerge = await cluster.MergeRanges(
            new GrpcMergeRangesRequest(), new StubServerCallContext());

        Assert.Equal("Completed", restMerge.Status);
        Assert.Equal(restMerge.Success, grpcMerge.Success);
        Assert.Equal(restMerge.Status, grpcMerge.Status);
        Assert.Equal(restMerge.Determinate, grpcMerge.Determinate);
        Assert.Equal(restMerge.Merges, grpcMerge.Merges);
        Assert.Equal(restMerge.Reason ?? "", grpcMerge.Reason);

        // Removal is idempotent, so both legs can run it against the same already-absent space.
        const string absent = "transport/absent";
        KahunaRemoveKeyRangeResponse restRemove = await kahuna.RemoveKeyRangeWithOutcomeAsync(absent, ct);
        GrpcRemoveKeyRangeResponse grpcRemove = await keyValues.RemoveKeyRange(
            new GrpcRemoveKeyRangeRequest { KeySpace = absent }, new StubServerCallContext());

        Assert.Equal(restRemove.Success, grpcRemove.Success);
        Assert.Equal(restRemove.Status, grpcRemove.Status);
        Assert.Equal(restRemove.RoutingMode, grpcRemove.RoutingMode);
        Assert.Equal(restRemove.DescriptorCount, grpcRemove.DescriptorCount);
        Assert.Equal(restRemove.Reason ?? "", grpcRemove.Reason);
    }

    /// <summary>
    /// The client methods an operator tool is built on, driven end to end against an embedded node:
    /// register, read, split, merge, unregister. The split here is a real one, so this also proves
    /// the client surfaces enough to act on — the new partition id it reports is the one the map
    /// then shows serving the upper half.
    /// </summary>
    [Fact]
    public async Task ClientMethods_DriveTheFullRangeAdminLifecycle()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string space = "client/admin/" + Guid.NewGuid().ToString("N")[..8];

        KahunaRegisterKeyRangeResponse registered = await client.RegisterKeyRange(space, cancellationToken: ct);
        Assert.True(registered.Success);
        Assert.True(registered.Seeded);
        Assert.Equal(1, registered.DescriptorCount);

        KahunaRangeMapResponse map = await client.GetRanges(space, cancellationToken: ct);
        KahunaKeySpaceRangesResponse entry = Assert.Single(map.KeySpaces);
        Assert.Equal("KeyRange", entry.RoutingMode);
        Assert.Single(entry.Descriptors);
        Assert.Null(entry.Descriptors[0].StartKey);
        Assert.Null(entry.Descriptors[0].EndKey);

        // Both halves need keys, or the splitter refuses rather than creating an empty range.
        foreach (string key in (string[])[$"{space}/a", $"{space}/b", $"{space}/p", $"{space}/q"])
            await client.SetKeyValue(key, key, 0, KeyValueFlags.Set, KeyValueDurability.Persistent, ct);

        KahunaSplitRangeResponse split = await client.SplitRange(space, $"{space}/m", cancellationToken: ct);
        Assert.True(split.Success, $"split refused: {split.Status} — {split.Reason}");
        Assert.True(split.Determinate);
        Assert.True(split.NewPartitionId > 0);

        map = await client.GetRanges(space, cancellationToken: ct);
        List<KahunaRangeDescriptorResponse> descriptors = map.KeySpaces[0].Descriptors;
        Assert.Equal(2, descriptors.Count);
        Assert.Equal($"{space}/m", descriptors[0].EndKey);
        Assert.Equal($"{space}/m", descriptors[1].StartKey);
        Assert.Equal(split.NewPartitionId, descriptors[1].PartitionId);

        // Two keys per half is under the default minimum, so the pass folds them straight back.
        KahunaMergeRangesResponse merged = await client.MergeRanges(cancellationToken: ct);
        Assert.True(merged.Success, $"merge refused: {merged.Status} — {merged.Reason}");
        Assert.Equal(1, merged.Merges);

        map = await client.GetRanges(space, cancellationToken: ct);
        Assert.Single(map.KeySpaces[0].Descriptors);

        KahunaRemoveKeyRangeResponse removed = await client.RemoveKeyRange(space, cancellationToken: ct);
        Assert.True(removed.Success);
        Assert.Equal("Removed", removed.Status);
        Assert.Equal(0, removed.DescriptorCount);
    }

    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}

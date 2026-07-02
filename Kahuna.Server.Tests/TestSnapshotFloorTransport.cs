
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

[Collection("ClusterTests")]
public sealed class TestSnapshotFloorTransport
{
    private readonly ILoggerFactory loggerFactory;

    public TestSnapshotFloorTransport(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    /// <summary>
    /// Verifies that acquiring a snapshot hold returns a successful response with a non-empty hold ID
    /// and a future lease expiry, and that GetSnapshotFloor reflects a live hold.
    /// </summary>
    [Fact]
    public async Task AcquireSnapshotHold_ReturnsSuccessAndHoldId()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        // Non-zero N and C so all three HLC components are in play.
        HLCTimestamp timestamp = new(17, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 3u);
        const int leaseMs = 60_000;

        (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) =
            await client.AcquireSnapshotHold("holder-1", timestamp, leaseMs, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.False(string.IsNullOrEmpty(holdId));
        Assert.True(leaseExpiry.L > timestamp.L);
    }

    /// <summary>
    /// Verifies the full acquire→get-floor→renew→release round-trip:
    /// the effective floor is non-zero after acquiring a hold, and falls back to zero after release.
    /// </summary>
    [Fact]
    public async Task SnapshotHold_RoundTrip_FloorMovesCorrectly()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        // Floor should be zero before any hold.
        (HLCTimestamp floorBefore, int holdsBefore) =
            await client.GetSnapshotFloor(TestContext.Current.CancellationToken);
        Assert.Equal(HLCTimestamp.Zero, floorBefore);
        Assert.Equal(0, holdsBefore);

        // Non-zero N and C so all three HLC components are in play.
        HLCTimestamp timestamp = new(29, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 11u);
        const int leaseMs = 60_000;

        (KeyValueResponseType acquireType, string holdId, HLCTimestamp leaseExpiry) =
            await client.AcquireSnapshotHold("holder-round-trip", timestamp, leaseMs, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, acquireType);
        Assert.True(leaseExpiry.L > timestamp.L);

        // Floor must be exactly the registered timestamp — not merely non-zero.
        (HLCTimestamp floorAfterAcquire, int holdsAfterAcquire) =
            await client.GetSnapshotFloor(TestContext.Current.CancellationToken);
        Assert.Equal(timestamp, floorAfterAcquire);
        Assert.Equal(1, holdsAfterAcquire);

        // Renew the hold.
        (KeyValueResponseType renewType, HLCTimestamp newExpiry) =
            await client.RenewSnapshotHold(holdId, leaseMs, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, renewType);
        Assert.True(newExpiry.L > timestamp.L);

        // Release the hold.
        KeyValueResponseType releaseType =
            await client.ReleaseSnapshotHold(holdId, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Deleted, releaseType);

        // Floor should be zero again.
        (HLCTimestamp floorAfterRelease, int holdsAfterRelease) =
            await client.GetSnapshotFloor(TestContext.Current.CancellationToken);
        Assert.Equal(HLCTimestamp.Zero, floorAfterRelease);
        Assert.Equal(0, holdsAfterRelease);
    }

    /// <summary>
    /// Verifies that acquiring the same (holderId, timestamp) pair a second time is idempotent:
    /// the same holdId is returned and the response type is Set.
    /// </summary>
    [Fact]
    public async Task AcquireSnapshotHold_Idempotent_SameHolderId_And_Timestamp()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        // Non-zero N and C so all three HLC components are in play.
        HLCTimestamp timestamp = new(7, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 5u);
        const int leaseMs = 60_000;

        (KeyValueResponseType type1, string holdId1, _) =
            await client.AcquireSnapshotHold("holder-idempotent", timestamp, leaseMs, TestContext.Current.CancellationToken);

        (KeyValueResponseType type2, string holdId2, _) =
            await client.AcquireSnapshotHold("holder-idempotent", timestamp, leaseMs, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, type1);
        Assert.Equal(KeyValueResponseType.Set, type2);
        Assert.Equal(holdId1, holdId2);

        // Only one live hold should exist.
        (_, int liveHolds) = await client.GetSnapshotFloor(TestContext.Current.CancellationToken);
        Assert.Equal(1, liveHolds);
    }
}

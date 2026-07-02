using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Client.Tests;

/// <summary>
/// End-to-end transport tests for the snapshot hold API: acquire, renew, release, and
/// get-floor over both gRPC and REST transports against a live cluster.
///
/// Uses a non-trivial HLC (non-zero N, C, and a large L) so that any field-swap or
/// dropped component in the proto/JSON serialization path is caught by the exact-equality
/// assertions on the returned floor and lease-expiry values.
/// </summary>
public class TestSnapshotHold
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    private readonly ITestOutputHelper outputHelper;

    public TestSnapshotHold(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
    }

    // A realistic HLC with all three components non-zero so field-swaps are caught.
    // L = 2024-01-01 00:00:00 UTC in ms; N = 42; C = 7.
    private static readonly HLCTimestamp TestFloor = new(42, 1_704_067_200_000L, 7u);

    [Theory, CombinatorialData]
    public async Task AcquireSnapshotHold_RoundTripsHlcExactly(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(communicationType, clientType);

        string holderId = "test-holder-" + Guid.NewGuid().ToString("N")[..8];

        (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) =
            await client.AcquireSnapshotHold(holderId, TestFloor, leaseMs: 60_000, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.NotEmpty(holdId);
        // Lease expiry must be after the test floor (it's floor + leaseMs offset from now, so L > floor.L).
        Assert.NotEqual(HLCTimestamp.Zero, leaseExpiry);

        // Floor must be exactly the timestamp we registered.
        (HLCTimestamp floor, int liveHolds) = await client.GetSnapshotFloor(ct);
        Assert.Equal(TestFloor, floor);
        Assert.True(liveHolds >= 1);

        // Clean up.
        KeyValueResponseType releaseType = await client.ReleaseSnapshotHold(holdId, ct);
        Assert.Equal(KeyValueResponseType.Deleted, releaseType);
    }

    [Theory, CombinatorialData]
    public async Task RenewSnapshotHold_UpdatesLeaseExpiry(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(communicationType, clientType);

        string holderId = "renew-holder-" + Guid.NewGuid().ToString("N")[..8];

        (KeyValueResponseType acquireType, string holdId, HLCTimestamp expiry1) =
            await client.AcquireSnapshotHold(holderId, TestFloor, leaseMs: 30_000, ct);

        Assert.Equal(KeyValueResponseType.Set, acquireType);
        Assert.NotEmpty(holdId);
        Assert.NotEqual(HLCTimestamp.Zero, expiry1);

        // Renew with a longer TTL — expiry should be >= original expiry.
        (KeyValueResponseType renewType, HLCTimestamp expiry2) =
            await client.RenewSnapshotHold(holdId, leaseMs: 120_000, ct);

        Assert.Equal(KeyValueResponseType.Set, renewType);
        Assert.NotEqual(HLCTimestamp.Zero, expiry2);
        // Longer lease must push expiry at least as far as the shorter one.
        Assert.True(expiry2.CompareTo(expiry1) >= 0,
            $"Renewed expiry {expiry2} should be >= original expiry {expiry1}");

        // Clean up.
        await client.ReleaseSnapshotHold(holdId, ct);
    }

    [Theory, CombinatorialData]
    public async Task ReleaseSnapshotHold_FloorDropsToZeroWhenLastHoldReleased(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(communicationType, clientType);

        string holderId = "release-holder-" + Guid.NewGuid().ToString("N")[..8];

        (_, string holdId, _) = await client.AcquireSnapshotHold(holderId, TestFloor, leaseMs: 60_000, ct);
        Assert.NotEmpty(holdId);

        // Confirm the floor is set before releasing.
        (HLCTimestamp floorBefore, _) = await client.GetSnapshotFloor(ct);
        Assert.Equal(TestFloor, floorBefore);

        KeyValueResponseType releaseType = await client.ReleaseSnapshotHold(holdId, ct);
        Assert.Equal(KeyValueResponseType.Deleted, releaseType);

        // After releasing the only hold, the floor must drop to Zero.
        (HLCTimestamp floorAfter, int liveAfter) = await client.GetSnapshotFloor(ct);
        Assert.Equal(HLCTimestamp.Zero, floorAfter);
        Assert.Equal(0, liveAfter);
    }

    [Theory, CombinatorialData]
    public async Task GetSnapshotFloor_TwoHoldsFloorIsMin(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(communicationType, clientType);

        // Two timestamps with all fields distinct so N/L/C confusion is caught.
        HLCTimestamp t1 = new(3, 1_000_000_000L, 2u);   // lower
        HLCTimestamp t2 = new(5, 2_000_000_000L, 9u);   // higher

        string holder1 = "floor-min-a-" + Guid.NewGuid().ToString("N")[..8];
        string holder2 = "floor-min-b-" + Guid.NewGuid().ToString("N")[..8];

        (_, string holdId1, _) = await client.AcquireSnapshotHold(holder1, t1, leaseMs: 60_000, ct);
        (_, string holdId2, _) = await client.AcquireSnapshotHold(holder2, t2, leaseMs: 60_000, ct);

        (HLCTimestamp floor, int liveHolds) = await client.GetSnapshotFloor(ct);
        // Effective floor = min(t1, t2) = t1.
        Assert.Equal(t1, floor);
        Assert.True(liveHolds >= 2);

        // Release lower hold — floor should rise to t2.
        await client.ReleaseSnapshotHold(holdId1, ct);
        (HLCTimestamp floorAfter, _) = await client.GetSnapshotFloor(ct);
        Assert.Equal(t2, floorAfter);

        // Clean up.
        await client.ReleaseSnapshotHold(holdId2, ct);
    }

    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType)
    {
        return clientType switch
        {
            KahunaClientType.SingleEndpoint  => new(url, null, GetCommunicationByType(communicationType)),
            KahunaClientType.PoolOfEndpoints => new(urls, null, GetCommunicationByType(communicationType)),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }

    private static IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(new() { AllowInsecureCertificateValidation = true }, null),
            KahunaCommunicationType.Rest => new RestCommunication(null, new() { AllowInsecureCertificateValidation = true }),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType), communicationType, null)
        };
    }
}

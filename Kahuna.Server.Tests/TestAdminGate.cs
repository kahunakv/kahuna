using System.Net;
using System.Net.Sockets;
using System.Text;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Communication;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The admin credential: a node with admin keys refuses its admin operations — node removal, replication factor,
/// range-map DDL, backups and restore — to a caller without one, on REST and on gRPC, before the operation runs.
/// Data and read-only calls never need a key, and a node without keys keeps every operation open. Driven through a
/// real Kestrel host wired by the same <c>AddAdminGate</c> the server uses, and through the real client transports.
/// </summary>
public sealed class TestAdminGate
{
    private const string KeyA = "admin-key-a-0123456789abcdef0123456789";

    private const string KeyB = "admin-key-b-0123456789abcdef0123456789";

    /// <summary>Every gated REST route, with a body that binds, so a refusal can only come from the gate.</summary>
    private static readonly string[] RestAdminRoutes =
    [
        "/v1/cluster/leave",
        "/v1/cluster/replication-factor",
        "/v1/ranges/register",
        "/v1/ranges/unregister",
        "/v1/ranges/split",
        "/v1/ranges/merge",
        "/v1/backups/full",
        "/v1/backups/incremental",
        "/v1/backups/coordinated",
        "/v1/backups/gc?dryRun=true",
        "/v1/backups/validate-chain",
        "/v1/restore"
    ];

    private readonly ILoggerFactory loggerFactory;

    public TestAdminGate(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task RestAdminRoutes_RefuseACallerWithoutAValidKey_AndRunNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await KahunaTestHost.StartAsync(loggerFactory, ct, [KeyA, KeyB]);
        using HttpClient http = new();

        foreach (string route in RestAdminRoutes)
        {
            Assert.Equal(HttpStatusCode.Unauthorized, (await Post(http, host.RestUrl + route, null, "{\"keySpace\":\"t:refused\",\"splitKey\":\"t:refused/m\"}", ct)).StatusCode);
            Assert.Equal(HttpStatusCode.Unauthorized, (await Post(http, host.RestUrl + route, "not-a-configured-admin-key-000000000", "{}", ct)).StatusCode);
        }

        // The refused register never reached the range map, and the refused leave left the node serving.
        string ranges = await http.GetStringAsync(host.RestUrl + "/v1/ranges", ct);
        Assert.DoesNotContain("t:refused", ranges);
        Assert.Equal(HttpStatusCode.OK, (await Post(http, host.RestUrl + "/v1/kv/try-set", null, "{\"key\":\"t:data/k\",\"value\":\"AQ==\",\"flags\":1}", ct)).StatusCode);

        // Either key of a rotation is admitted.
        Assert.NotEqual(HttpStatusCode.Unauthorized, (await Post(http, host.RestUrl + "/v1/ranges/merge", KeyA, "", ct)).StatusCode);
        Assert.NotEqual(HttpStatusCode.Unauthorized, (await Post(http, host.RestUrl + "/v1/ranges/merge", KeyB, "", ct)).StatusCode);

        // Read-only admin views stay open.
        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync(host.RestUrl + "/v1/cluster/membership", ct)).StatusCode);
        Assert.NotEqual(HttpStatusCode.Unauthorized, (await http.GetAsync(host.RestUrl + "/v1/backups", ct)).StatusCode);
    }

    [Fact]
    public async Task GrpcAdminMethods_RefuseACallerWithoutAValidKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await KahunaTestHost.StartAsync(loggerFactory, ct, [KeyA, KeyB]);
        using GrpcChannel channel = GrpcChannel.ForAddress(host.GrpcUrl);

        Cluster.ClusterClient cluster = new(channel);
        Backups.BackupsClient backups = new(channel);
        KeyValuer.KeyValuerClient keyValues = new(channel);
        Metadata wrong = new() { { KahunaAdminCredential.MetadataName, "not-a-configured-admin-key-000000000" } };

        await AssertUnauthenticated(cluster.MergeRangesAsync(new GrpcMergeRangesRequest(), cancellationToken: ct));
        await AssertUnauthenticated(cluster.MergeRangesAsync(new GrpcMergeRangesRequest(), wrong, cancellationToken: ct));
        await AssertUnauthenticated(cluster.LeaveAsync(new GrpcClusterLeaveRequest(), cancellationToken: ct));
        await AssertUnauthenticated(cluster.SplitRangeAsync(new GrpcSplitRangeRequest { KeySpace = "t:refused", SplitKey = "t:refused/m" }, cancellationToken: ct));
        await AssertUnauthenticated(backups.TakeFullBackupAsync(new GrpcTakeFullBackupRequest(), cancellationToken: ct));
        await AssertUnauthenticated(backups.RestoreAsync(new GrpcRestoreRequest(), cancellationToken: ct));
        await AssertUnauthenticated(keyValues.RegisterKeyRangeAsync(new GrpcRegisterKeyRangeRequest { KeySpace = "t:refused" }, cancellationToken: ct));

        GrpcGetRangesResponse ranges = await cluster.GetRangesAsync(new GrpcGetRangesRequest(), cancellationToken: ct);
        Assert.DoesNotContain("t:refused", ranges.ToString());

        GrpcTrySetKeyValueResponse set = await keyValues.TrySetKeyValueAsync(new GrpcTrySetKeyValueRequest
        {
            Key = "t:data/k",
            Value = Google.Protobuf.ByteString.CopyFrom([1]),
            Flags = GrpcKeyValueFlags.Set,
            Durability = GrpcKeyValueDurability.Persistent
        }, cancellationToken: ct);
        Assert.Equal(GrpcKeyValueResponseType.TypeSet, set.Type);

        foreach (string key in new[] { KeyA, KeyB })
        {
            GrpcMergeRangesResponse merged = await cluster.MergeRangesAsync(
                new GrpcMergeRangesRequest(), new Metadata { { KahunaAdminCredential.MetadataName, key } }, cancellationToken: ct);
            Assert.False(string.IsNullOrEmpty(merged.Status));
        }
    }

    [Theory]
    [InlineData("grpc")]
    [InlineData("rest")]
    public async Task ClientLibrary_SendsTheAdminKeyOnAdminCalls(string transport)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await KahunaTestHost.StartAsync(loggerFactory, ct, [KeyA]);

        KahunaClient withKey = host.Client(transport, KeyA);
        KahunaMergeRangesResponse merged = await withKey.MergeRanges(cancellationToken: ct);
        Assert.False(string.IsNullOrEmpty(merged.Status));

        KahunaClient withoutKey = host.Client(transport, null);
        Exception refused = await Assert.ThrowsAnyAsync<Exception>(() => withoutKey.MergeRanges(cancellationToken: ct));

        if (transport == "grpc")
            Assert.Equal(StatusCode.Unauthenticated, Assert.IsType<RpcException>(refused).StatusCode);
        else
            Assert.Contains("admin key", Assert.IsType<KahunaException>(refused).Message);
    }

    [Fact]
    public async Task NoKeys_LeaveTheAdminOperationsOpen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await KahunaTestHost.StartAsync(loggerFactory, ct, []);
        using HttpClient http = new();
        using GrpcChannel channel = GrpcChannel.ForAddress(host.GrpcUrl);

        Assert.NotEqual(HttpStatusCode.Unauthorized, (await Post(http, host.RestUrl + "/v1/ranges/merge", null, "", ct)).StatusCode);

        GrpcMergeRangesResponse merged = await new Cluster.ClusterClient(channel).MergeRangesAsync(new GrpcMergeRangesRequest(), cancellationToken: ct);
        Assert.False(string.IsNullOrEmpty(merged.Status));
    }

    [Fact]
    public void GrpcAdminMethods_AreTheAdminOperationsAndNoNodeToNodeCall()
    {
        Assert.Equal(
            new HashSet<string>
            {
                "/Cluster/Leave", "/Cluster/SetReplicationFactor", "/Cluster/SplitRange", "/Cluster/MergeRanges",
                "/Backups/TakeFullBackup", "/Backups/TakeIncrementalBackup", "/Backups/TakeCoordinatedBackup",
                "/Backups/ValidateChain", "/Backups/Restore", "/Backups/RunBackupGarbageCollection",
                "/KeyValuer/RegisterKeyRange", "/KeyValuer/RemoveKeyRange"
            },
            AdminGate.GrpcAdminMethods.ToHashSet());

        Assert.False(AdminGate.GrpcAdminMethods.Contains("/Backups/CaptureBackupPart"));
        Assert.False(AdminGate.GrpcAdminMethods.Contains("/KeyValuer/BatchServerKeyValueRequests"));
    }

    [Fact]
    public void KeyFile_IsReadOrStopsTheNode()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna-admin-keys-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);

        try
        {
            string good = Path.Combine(dir, "good");
            File.WriteAllText(good, $"# rotation\n\n{KeyA}\n  {KeyB}  \n");
            Assert.Equal([KeyA, KeyB], AdminGate.LoadKeys(good));

            Assert.Empty(AdminGate.LoadKeys(""));

            string empty = Path.Combine(dir, "empty");
            File.WriteAllText(empty, "# nothing here\n\n");
            Assert.Throws<KahunaServerException>(() => AdminGate.LoadKeys(empty));

            string weak = Path.Combine(dir, "weak");
            File.WriteAllText(weak, "short\n");
            Assert.Throws<KahunaServerException>(() => AdminGate.LoadKeys(weak));

            Assert.Throws<KahunaServerException>(() => AdminGate.LoadKeys(Path.Combine(dir, "missing")));

            AdminGate gate = new([KeyA, KeyB], NullLogger<AdminGate>.Instance);
            Assert.True(gate.IsAuthorized(KeyA));
            Assert.True(gate.IsAuthorized(KeyB));
            Assert.False(gate.IsAuthorized(null));
            Assert.False(gate.IsAuthorized(KeyA + "x"));
            Assert.True(AdminGate.Disabled.IsAuthorized(null));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static async Task<HttpResponseMessage> Post(HttpClient http, string url, string? adminKey, string body, CancellationToken ct)
    {
        using HttpRequestMessage request = new(HttpMethod.Post, url);
        request.Content = new StringContent(body, Encoding.UTF8, "application/json");

        if (adminKey is not null)
            request.Headers.Add(KahunaAdminCredential.HeaderName, adminKey);

        return await http.SendAsync(request, ct);
    }

    private static async Task AssertUnauthenticated<T>(AsyncUnaryCall<T> call)
    {
        RpcException ex = await Assert.ThrowsAsync<RpcException>(async () => await call);
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }
}

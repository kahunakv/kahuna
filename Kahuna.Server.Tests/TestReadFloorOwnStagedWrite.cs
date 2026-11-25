using System.Net;
using System.Net.Sockets;
using System.Text;

using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transaction with <see cref="KeyValueTransactionOptions.ReadFloor"/> that reads a key it wrote. The answer comes
/// from its own staged write (a set, a delete, or a value whose time to live ran out) while the committed head is
/// still the one below it, so it is not judged against the floor, whichever read asks for it and whichever node
/// answers. An absent key that the transaction staged nothing on is judged like any other.
/// </summary>
public sealed class TestReadFloorOwnStagedWrite : BaseCluster, IClassFixture<TestReadFloorOwnStagedWrite.SharedNode>
{
    /// <summary>The registered latest reads that are held to a read floor.</summary>
    public enum ReadShape
    {
        Get,
        Exists,
        Probe
    }

    private static readonly ReadShape[] Shapes = [ReadShape.Get, ReadShape.Exists, ReadShape.Probe];

    private readonly SharedNode fixture;

    private readonly ILoggerFactory loggerFactory;

    public TestReadFloorOwnStagedWrite(ITestOutputHelper outputHelper, SharedNode fixture)
    {
        this.fixture = fixture;
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    /// <summary>The node of the cases that need one node. Every test uses keys of its own.</summary>
    public sealed class SharedNode : IAsyncLifetime
    {
        private readonly EmbeddedKahunaNode node = new(NodeOptions());

        public IKahuna Kahuna => node.Kahuna;

        public async ValueTask InitializeAsync()
        {
            await node.StartAsync(TestContext.Current.CancellationToken);
            await node.WaitForLeaderForKeyAsync("ownstaged/x", TestContext.Current.CancellationToken);
        }

        public ValueTask DisposeAsync() => node.DisposeAsync();
    }

    // ── one node ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The transaction deletes a key it has not read and reads it back: it is absent, the transaction is not refused,
    /// and it commits. The second and the third read of the key run through the other two shapes.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task KeyDeletedUnread_IsAbsent_AndTheTransactionReadsOnAndCommits(ReadShape shape, bool earlierRead, KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct, durability);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);

        if (earlierRead)
            Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct, durability)).Value);

        Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Written, ct, durability));

        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Written, ct, durability)).Type);

        // The transaction is not refused: a key it has not touched is answered, and so is its own delete again.
        Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct, durability)).Value);

        foreach (ReadShape again in Shapes)
            Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, again, keys.Written, ct, durability)).Type);

        // The absence it read back is its own write: the commit validates it as a write, and not as a read of a
        // missing key, which the committed value would contradict.
        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(tx.CoordinatorKey, tx.TransactionId, ct);
        Assert.NotNull(workingSet);
        Assert.Equal([keys.Written], workingSet.ModifiedKeys.Select(static modified => modified.Key));
        Assert.DoesNotContain(workingSet.ReadKeys, read => read.Key == keys.Written);
        Assert.Contains(workingSet.ReadKeys, read => read.Key == keys.Other);

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));

        Assert.Null(await CommittedValue(kahuna, keys.Written, ct, durability));
        Assert.Equal("other", await CommittedValue(kahuna, keys.Other, ct, durability));
    }

    /// <summary>A delete of several keys in one batch stages each of them as a single delete does.</summary>
    [Theory, CombinatorialData]
    public async Task KeysDeletedUnreadInOneBatch_AreAbsent(ReadShape shape)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);

        List<KahunaDeleteKeyValueResponseItem> deleted = await kahuna.LocateAndTryDeleteManyKeyValue(
            [
                new() { TransactionId = tx.TransactionId, Key = keys.Written, Durability = KeyValueDurability.Persistent },
                new() { TransactionId = tx.TransactionId, Key = keys.Other, Durability = KeyValueDurability.Persistent }
            ], ct, tx.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.All(deleted, static item => Assert.Equal(KeyValueResponseType.Deleted, item.Type));

        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Written, ct)).Type);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Other, ct)).Type);
        Assert.Equal("third", (await Read(kahuna, tx, ReadShape.Get, keys.Third, ct)).Value);

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
        Assert.Null(await CommittedValue(kahuna, keys.Written, ct));
        Assert.Null(await CommittedValue(kahuna, keys.Other, ct));
    }

    /// <summary>One probe of a key the transaction deleted unread, a key it set unread and a key it did not write.</summary>
    [Fact]
    public async Task ProbeOfOwnWritesAndAnUnwrittenKey_AnswersEach()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);
        Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Written, ct));
        Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, tx, keys.Third, "staged", ct));

        KeyValueProbeResult[] results = await Probe(kahuna, tx, [keys.Written, keys.Third, keys.Other], ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, results[0].Type);
        Assert.Equal("staged", Encoding.UTF8.GetString(results[1].Entry!.Value!));
        Assert.Equal("other", Encoding.UTF8.GetString(results[2].Entry!.Value!));

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
    }

    /// <summary>Deleted unread, then set again: the read answers the staged value, and the value is what commits.</summary>
    [Theory, CombinatorialData]
    public async Task KeyDeletedUnreadThenSet_ReadsTheStagedValue(ReadShape shape, bool readBetween)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Written, ct));

        if (readBetween)
            Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Written, ct)).Type);

        Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, tx, keys.Written, "again", ct));

        AssertServed(shape, "again", await Read(kahuna, tx, shape, keys.Written, ct));
        Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct)).Value);

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
        Assert.Equal("again", await CommittedValue(kahuna, keys.Written, ct));
    }

    /// <summary>A key the transaction set unread is answered from the staged value by every shape.</summary>
    [Theory, CombinatorialData]
    public async Task KeySetUnread_ReadsTheStagedValue_AndTheTransactionReadsOnAndCommits(ReadShape shape, bool earlierRead)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);

        if (earlierRead)
            Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);

        Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, tx, keys.Written, "staged", ct));

        AssertServed(shape, "staged", await Read(kahuna, tx, shape, keys.Written, ct));
        Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct)).Value);

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
        Assert.Equal("staged", await CommittedValue(kahuna, keys.Written, ct));
    }

    /// <summary>A key the transaction inserted and then deleted is absent, and nothing of it commits.</summary>
    [Theory, CombinatorialData]
    public async Task KeyInsertedThenDeleted_IsAbsent(ReadShape shape)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);
        Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, tx, keys.Missing, "short", ct));
        Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Missing, ct));

        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Missing, ct)).Type);
        Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct)).Value);

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
        Assert.Null(await CommittedValue(kahuna, keys.Missing, ct));
    }

    /// <summary>
    /// A staged value whose time to live runs out inside the transaction is absent because of the transaction's own
    /// write: the committed value below it says nothing about the answer.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task StagedValueThatExpired_IsAbsent_AndTheTransactionReadsOn(ReadShape shape, bool extend)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);

        if (extend)
        {
            (KeyValueResponseType extended, _, _) = await kahuna.LocateAndTryExtendKeyValue(
                tx.TransactionId, keys.Written, 50, KeyValueDurability.Persistent, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Extended, extended);
        }
        else
            Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, tx, keys.Written, "short", ct, expiresMs: 50));

        await WaitUntilAsync(async () => (await Read(kahuna, tx, shape, keys.Written, ct)).Type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists));

        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Written, ct)).Type);
        Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct)).Value);
        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
    }

    /// <summary>A key read, deleted and read again is absent.</summary>
    [Theory, CombinatorialData]
    public async Task KeyReadThenDeleted_IsAbsent_AndTheTransactionCommits(ReadShape shape)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        AssertServed(shape, "written", await Read(kahuna, tx, shape, keys.Written, ct));
        Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Written, ct));
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Written, ct)).Type);
        Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct)).Value);

        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
        Assert.Null(await CommittedValue(kahuna, keys.Written, ct));
    }

    /// <summary>
    /// How the key whose read must be judged came to be absent, and what the transaction did to it. None of these
    /// stages a write on it.
    /// </summary>
    public enum AbsentKey
    {
        NeverWritten,
        DeletedByAnother,
        ExpiredBefore,
        DeleteFoundNothing,
        Locked
    }

    /// <summary>
    /// Control: an absent key the transaction staged no write on is judged. Another transaction changed a key this
    /// one read before, so its absence is not handed out.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task AbsentKeyWithNoStagedWrite_IsRefusedAfterAChangeOfAnEarlierKey(ReadShape shape, AbsentKey absent)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        switch (absent)
        {
            case AbsentKey.DeletedByAnother:
                await AutoCommitSet(kahuna, keys.Missing, "gone", ct);
                (KeyValueResponseType deleted, _, _) = await RetryOnMustRetryAsync(
                    () => kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, keys.Missing, KeyValueDurability.Persistent, ct),
                    static answer => answer.Item1);
                Assert.Equal(KeyValueResponseType.Deleted, deleted);
                break;

            case AbsentKey.ExpiredBefore:
                await AutoCommitSet(kahuna, keys.Missing, "gone", ct, expiresMs: 50);
                await WaitUntilAsync(async () => await CommittedValue(kahuna, keys.Missing, ct) is null);
                break;
        }

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);

        switch (absent)
        {
            case AbsentKey.DeleteFoundNothing:
                Assert.Equal(KeyValueResponseType.DoesNotExist, await Delete(kahuna, tx, keys.Missing, ct));
                break;

            case AbsentKey.Locked:
                (KeyValueResponseType locked, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                    tx.TransactionId, keys.Missing, 30_000, KeyValueDurability.Persistent, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Locked, locked);
                break;
        }

        await Overwrite(kahuna, keys, ct);

        Assert.Equal(KeyValueResponseType.Aborted, (await Read(kahuna, tx, shape, keys.Missing, ct)).Type);
        Assert.NotEqual(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
    }

    /// <summary>
    /// Control: the transaction's own delete hides no change of a key it read before. The read of the deleted key is
    /// answered from the delete, and the next read of an unwritten key is refused.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task OwnDelete_DoesNotHideAChangeOfAnEarlierKey(ReadShape shape, bool oneProbe)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IKahuna kahuna = fixture.Kahuna;
        Keys keys = await Keys.Seed(kahuna, ct);

        TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
        Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);
        Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Written, ct));

        await Overwrite(kahuna, keys, ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(kahuna, tx, shape, keys.Written, ct)).Type);

        if (oneProbe)
        {
            KeyValueProbeResult[] results = await Probe(kahuna, tx, [keys.Written, keys.Other], ct);
            Assert.All(results, static result => Assert.Equal(KeyValueResponseType.Aborted, result.Type));
        }
        else
            Assert.Equal(KeyValueResponseType.Aborted, (await Read(kahuna, tx, shape, keys.Other, ct)).Type);

        Assert.NotEqual(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));
        Assert.Equal("written", await CommittedValue(kahuna, keys.Written, ct));
    }

    // ── three nodes ──────────────────────────────────────────────────────────

    /// <summary>
    /// The node the transaction is started on leads the keys, or another node does and every read and write of the
    /// transaction is forwarded to it, over the in-memory transport or over the production gRPC one. The mark of an
    /// own staged write comes back with the answer, so the node that holds the floor does not judge it.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task OwnStagedWrites_AreNotJudged_WhicheverNodeLeadsTheKeys(bool grpc, bool remoteLeader)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestCluster cluster = await TestCluster.Start(grpc, loggerFactory, ct);

        foreach (ReadShape shape in Shapes)
            await RunUnderStableLeadership(cluster.Raft, TestCluster.Partitions, async () =>
            {
                (Keys keys, IKahuna kahuna) = await cluster.SeedAndPickNode(remoteLeader, ct);

                TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
                Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);

                Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Written, ct));
                Assert.Equal((shape, KeyValueResponseType.DoesNotExist), (shape, (await Read(kahuna, tx, shape, keys.Written, ct)).Type));

                Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, tx, keys.Third, "staged", ct));
                AssertServed(shape, "staged", await Read(kahuna, tx, shape, keys.Third, ct));

                // The transaction is not refused, and what it wrote is what commits.
                Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct)).Value);
                Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));

                foreach (IKahuna reader in cluster.Kahunas)
                {
                    Assert.Null(await CommittedValue(reader, keys.Written, ct));
                    Assert.Equal("staged", await CommittedValue(reader, keys.Third, ct));
                }

                await cluster.AssertLeadership(kahuna, keys.Written, leads: !remoteLeader, ct);
            });
    }

    /// <summary>The forwarded read of a staged set alone: no read of a staged delete comes before it.</summary>
    [Theory, CombinatorialData]
    public async Task OwnStagedSet_ReadThroughTheGrpcTransport_IsNotJudged(bool remoteLeader)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestCluster cluster = await TestCluster.Start(grpc: true, loggerFactory, ct);

        foreach (ReadShape shape in Shapes)
            await RunUnderStableLeadership(cluster.Raft, TestCluster.Partitions, async () =>
            {
                (Keys keys, IKahuna kahuna) = await cluster.SeedAndPickNode(remoteLeader, ct);

                TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
                Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, tx, keys.Third, "staged", ct));
                AssertServed(shape, "staged", await Read(kahuna, tx, shape, keys.Third, ct));
                Assert.Equal("other", (await Read(kahuna, tx, ReadShape.Get, keys.Other, ct)).Value);
                Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));

                Assert.Equal("staged", await CommittedValue(kahuna, keys.Third, ct));
                await cluster.AssertLeadership(kahuna, keys.Written, leads: !remoteLeader, ct);
            });
    }

    /// <summary>
    /// Control: on three nodes too, an absent key the transaction staged nothing on is judged, and its own delete
    /// hides no change of a key it read before.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task ChangeOfAnEarlierKey_IsRefused_WhicheverNodeLeadsTheKeys(bool grpc, bool remoteLeader)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestCluster cluster = await TestCluster.Start(grpc, loggerFactory, ct);

        foreach (ReadShape shape in Shapes)
            await RunUnderStableLeadership(cluster.Raft, TestCluster.Partitions, async () =>
            {
                (Keys keys, IKahuna kahuna) = await cluster.SeedAndPickNode(remoteLeader, ct);

                TransactionHandle tx = await Begin(kahuna, keys.Prefix + "tx", ct);
                Assert.Equal("earlier", (await Read(kahuna, tx, ReadShape.Get, keys.Earlier, ct)).Value);
                Assert.Equal(KeyValueResponseType.Deleted, await Delete(kahuna, tx, keys.Written, ct));

                await Overwrite(kahuna, keys, ct);

                Assert.Equal((shape, KeyValueResponseType.DoesNotExist), (shape, (await Read(kahuna, tx, shape, keys.Written, ct)).Type));
                Assert.Equal((shape, KeyValueResponseType.Aborted), (shape, (await Read(kahuna, tx, shape, keys.Missing, ct)).Type));
                Assert.NotEqual(KeyValueResponseType.Committed, await Commit(kahuna, tx, ct));

                Assert.Equal("written", await CommittedValue(kahuna, keys.Written, ct));
                await cluster.AssertLeadership(kahuna, keys.Written, leads: !remoteLeader, ct);
            });
    }

    /// <summary>
    /// Three nodes that replicate in memory. Their key-value calls to each other travel in memory, or over loopback
    /// through the production transport and the production services, where an answer is only what its message says.
    /// </summary>
    private sealed class TestCluster : IAsyncDisposable
    {
        public const int Partitions = 2;

        private const int Size = 3;

        private readonly List<EmbeddedKahunaNode> nodes = [];

        private readonly List<WebApplication> listeners = [];

        public IEnumerable<IKahuna> Kahunas => nodes.Select(static node => node.Kahuna);

        public IRaft Raft => nodes[0].Raft;

        public static async Task<TestCluster> Start(bool grpc, ILoggerFactory loggerFactory, CancellationToken ct)
        {
            TestCluster cluster = new();

            try
            {
                int[] ports = FreePorts();
                InMemoryCommunication raftTransport = new();
                MemoryInterNodeCommmunication memory = new();
                Dictionary<string, IRaft> rafts = [];
                Dictionary<string, IKahuna> kahunas = [];

                for (int i = 0; i < Size; i++)
                {
                    EmbeddedKahunaOptions options = NodeOptions();
                    options.NodeId = i + 1;
                    options.NodeName = $"ownstaged-{i + 1}";
                    options.Port = ports[i];
                    options.InitialPartitions = Partitions;

                    IInterNodeCommunication interNode = grpc
                        ? new GrpcInterNodeCommunication(
                            new KahunaConfiguration { InterNodeGrpcScheme = "http://" }, new RaftTransportSecurityOptions(),
                            NullLogger<GrpcInterNodeCommunication>.Instance)
                        : memory;

                    EmbeddedKahunaNode node = new(
                        options, interNode, raftTransport,
                        new StaticDiscovery([.. ports.Where(port => port != ports[i]).Select(static port => new RaftNode($"localhost:{port}"))]),
                        loggerFactory);

                    cluster.nodes.Add(node);
                    rafts[$"localhost:{ports[i]}"] = node.Raft;
                    kahunas[$"localhost:{ports[i]}"] = node.Kahuna;

                    if (grpc)
                        cluster.listeners.Add(await Listen(node, ports[i], ct));
                }

                raftTransport.SetNodes(rafts);
                memory.SetNodes(kahunas);

                await Task.WhenAll(cluster.nodes.Select(node => node.StartAsync(ct)));
                return cluster;
            }
            catch
            {
                await cluster.DisposeAsync();
                throw;
            }
        }

        /// <summary>
        /// Commits the keys of one test and picks the node the transaction is started on: the leader of the keys, or
        /// a node that forwards every call on them. Each key space routes to one partition, so one node leads all of
        /// its keys.
        /// </summary>
        public async Task<(Keys Keys, IKahuna Kahuna)> SeedAndPickNode(bool remoteLeader, CancellationToken ct)
        {
            Keys keys = await Keys.Seed(nodes[0].Kahuna, ct);
            int leader = -1;

            await WaitUntilAsync(async () =>
            {
                leader = await LeaderOf(keys.Written, ct);
                return leader >= 0;
            }, timeoutMs: 30_000);

            return (keys, nodes[remoteLeader ? (leader + 1) % Size : leader].Kahuna);
        }

        /// <summary>The leadership did not move while the test ran, so its calls took the path the case names.</summary>
        public async Task AssertLeadership(IKahuna kahuna, string key, bool leads, CancellationToken ct)
        {
            int leader = await LeaderOf(key, ct);
            Assert.True(leader >= 0);
            Assert.Equal(leads, ReferenceEquals(nodes[leader].Kahuna, kahuna));
        }

        private async Task<int> LeaderOf(string key, CancellationToken ct)
        {
            for (int i = 0; i < nodes.Count; i++)
            {
                int partition = ((KahunaManager)nodes[i].Kahuna).KeyValues.LocateDurablePartition(key).PartitionId;
                if (await nodes[i].Raft.AmILeaderIfHosted(partition, ct))
                    return i;
            }

            return -1;
        }

        private static async Task<WebApplication> Listen(EmbeddedKahunaNode node, int port, CancellationToken ct)
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(node.Kahuna);
            builder.Services.AddSingleton(node.Raft);
            builder.Services.AddSingleton(new KahunaConfiguration());
            builder.Services.AddSingleton(NodeTransportGate.Disabled);
            builder.Services.AddSingleton<ILogger<IKahuna>>(NullLogger<IKahuna>.Instance);
            builder.Services.AddGrpc();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, static listen => listen.Protocols = HttpProtocols.Http2));

            WebApplication app = builder.Build();
            app.MapGrpcKahunaRoutes();
            await app.StartAsync(ct);
            return app;
        }

        private static int[] FreePorts()
        {
            TcpListener[] reserved = [.. Enumerable.Range(0, Size).Select(static _ => new TcpListener(IPAddress.Loopback, 0))];

            foreach (TcpListener listener in reserved)
                listener.Start();

            int[] ports = [.. reserved.Select(static listener => ((IPEndPoint)listener.LocalEndpoint).Port)];

            foreach (TcpListener listener in reserved)
                listener.Dispose();

            return ports;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (EmbeddedKahunaNode node in nodes)
                await node.DisposeAsync();

            foreach (WebApplication listener in listeners)
            {
                using CancellationTokenSource stop = new(TimeSpan.FromSeconds(2));
                try { await listener.StopAsync(stop.Token); } catch (OperationCanceledException) { }
                await listener.DisposeAsync();
            }
        }
    }

    // ── reads and writes of a transaction ────────────────────────────────────

    private static string NewPrefix() => "ownstaged/" + Guid.NewGuid().ToString("N")[..8] + "/";

    /// <summary>The keys of one test, of one key space, and the values they hold when the transaction starts.</summary>
    private sealed record Keys(string Prefix, string Written, string Other, string Earlier, string Third, string Missing)
    {
        public static async Task<Keys> Seed(IKahuna kahuna, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent)
        {
            string prefix = NewPrefix();
            Keys keys = new(prefix, prefix + "written", prefix + "other", prefix + "earlier", prefix + "third", prefix + "missing");

            await AutoCommitSet(kahuna, keys.Written, "written", ct, durability);
            await AutoCommitSet(kahuna, keys.Other, "other", ct, durability);
            await AutoCommitSet(kahuna, keys.Earlier, "earlier", ct, durability);
            await AutoCommitSet(kahuna, keys.Third, "third", ct, durability);
            return keys;
        }
    }

    private static EmbeddedKahunaOptions NodeOptions() => new()
    {
        TimerInitialDelay = TimeSpan.FromMilliseconds(50),
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1,
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 4
    };

    private static async Task<TransactionHandle> Begin(IKahuna kahuna, string coordinatorKey, CancellationToken ct, bool readFloor = true)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Timeout = 30_000, Locking = KeyValueTransactionLocking.Optimistic, ReadFloor = readFloor },
                ct), static answer => answer.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task<KeyValueResponseType> Commit(IKahuna kahuna, TransactionHandle tx, CancellationToken ct) =>
        (await RetryOnMustRetryAsync(async () => await kahuna.LocateAndCommitTransaction(tx, ct), static answer => answer.Type)).Type;

    /// <summary>A registered latest read by one of the shapes. A read that answers <c>Exists</c> carries no value.</summary>
    private static async Task<(KeyValueResponseType Type, string? Value)> Read(
        IKahuna kahuna, TransactionHandle tx, ReadShape shape, string key, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(async () =>
        {
            switch (shape)
            {
                case ReadShape.Exists:
                    return await kahuna.LocateAndTryExistsValue(
                        tx.TransactionId, key, -1, HLCTimestamp.Zero, durability, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom());

                case ReadShape.Probe:
                    KeyValueProbeResult result = (await Probe(kahuna, tx, [key], ct, durability))[0];
                    return (result.Type, result.Entry);

                default:
                    return await kahuna.LocateAndTryGetValue(
                        tx.TransactionId, key, -1, HLCTimestamp.Zero, durability, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom());
            }
        }, static answer => answer.Item1);

        return (type, type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null);
    }

    private static Task<KeyValueProbeResult[]> Probe(
        IKahuna kahuna, TransactionHandle tx, string[] keys, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent) =>
        RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryProbeMany(
                tx.TransactionId, HLCTimestamp.Zero, keys, KeyValueProbeLock.None, 0, durability, ct,
                tx.CoordinatorKey, TransactionOperationId.NewRandom()),
            static results => Array.Exists(results, static result => result.Type == KeyValueResponseType.MustRetry)
                ? KeyValueResponseType.MustRetry
                : results[0].Type);

    private static void AssertServed(ReadShape shape, string expected, (KeyValueResponseType Type, string? Value) read)
    {
        if (shape == ReadShape.Exists)
            Assert.Equal(KeyValueResponseType.Exists, read.Type);
        else
            Assert.Equal((KeyValueResponseType.Get, expected), read);
    }

    private static async Task<KeyValueResponseType> Delete(
        IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent) =>
        (await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, key, durability, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom()),
            static answer => answer.Item1)).Item1;

    private static async Task<KeyValueResponseType> Set(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct, int expiresMs = 0) =>
        (await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, expiresMs,
                KeyValueDurability.Persistent, ct, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            static answer => answer.Item1)).Item1;

    private static async Task AutoCommitSet(
        IKahuna kahuna, string key, string value, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent, int expiresMs = 0)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, expiresMs, durability, ct),
            static answer => answer.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>Another transaction writes the key the test read first and the key it has not read, and commits.</summary>
    private static async Task Overwrite(IKahuna kahuna, Keys keys, CancellationToken ct)
    {
        TransactionHandle writer = await Begin(kahuna, keys.Prefix + "writer", ct, readFloor: false);
        Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, writer, keys.Earlier, "earlier2", ct));
        Assert.Equal(KeyValueResponseType.Set, await Set(kahuna, writer, keys.Other, "other2", ct));
        Assert.Equal(KeyValueResponseType.Committed, await Commit(kahuna, writer, ct));
    }

    /// <summary>The value a read outside any transaction answers, or null when the key does not exist.</summary>
    private static async Task<string?> CommittedValue(IKahuna kahuna, string key, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, durability, ct).AsTask(),
            static answer => answer.Item1);

        if (type == KeyValueResponseType.DoesNotExist)
            return null;

        Assert.Equal(KeyValueResponseType.Get, type);
        return Encoding.UTF8.GetString(entry!.Value!);
    }
}

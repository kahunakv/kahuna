using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the batched write path of non-transactional set-many: persistent keys destined for the same
/// Raft partition are proposed in a single ReplicateLogs (one WAL append + quorum round trip) instead of one
/// proposal per key, then completed per key. These drive the real entry point
/// (<see cref="KeyValuesManager.LocateAndTrySetManyKeyValue"/>) and assert the observable outcome — values
/// land, revisions advance by exactly one per write (no double-apply), and mixed durability still works.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestBatchedSetMany
{
    private readonly ILoggerFactory loggerFactory;

    public TestBatchedSetMany(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static KahunaSetKeyValueRequestItem SetItem(string key, byte[] value) => new()
    {
        Key = key,
        Value = value,
        ExpiresMs = 0,
        Flags = KeyValueFlags.Set,
        Durability = KeyValueDurability.Persistent
    };

    /// <summary>
    /// A batched set-many of many persistent keys writes every key, reports Set for each, and each key reads
    /// back its exact value at revision 0.
    /// </summary>
    [Fact]
    public async Task BatchedSetMany_WritesAllKeys_ReadsBackWithCorrectValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;

        const int count = 40;
        List<KahunaSetKeyValueRequestItem> items = [];
        for (int i = 0; i < count; i++)
            items.Add(SetItem($"bench/k{i}", Encoding.UTF8.GetBytes($"v{i}")));

        List<KahunaSetKeyValueResponseItem> results = await kv.LocateAndTrySetManyKeyValue(items, ct);

        Assert.Equal(count, results.Count);
        foreach (KahunaSetKeyValueResponseItem r in results)
            Assert.Equal(KeyValueResponseType.Set, r.Type);

        for (int i = 0; i < count; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"bench/k{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(entry);
            Assert.Equal($"v{i}", Encoding.UTF8.GetString(entry!.Value!));
            Assert.Equal(0, entry.Revision);
        }
    }

    /// <summary>
    /// Re-running a batched set-many over the same keys advances each key's revision by exactly one — the
    /// no-double-apply guarantee. A batched proposal commits N logs at once; if the leader applied both via
    /// the replicator and CompleteProposal, the revision would jump by more than one or the value would be
    /// wrong. Exercises three writes to be sure the increment is stable.
    /// </summary>
    [Fact]
    public async Task BatchedSetMany_RepeatedWrites_AdvanceRevisionByExactlyOne()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;

        const int count = 16;

        for (int round = 0; round < 3; round++)
        {
            List<KahunaSetKeyValueRequestItem> items = [];
            for (int i = 0; i < count; i++)
                items.Add(SetItem($"rev/k{i}", Encoding.UTF8.GetBytes($"round{round}-v{i}")));

            List<KahunaSetKeyValueResponseItem> results = await kv.LocateAndTrySetManyKeyValue(items, ct);

            Assert.Equal(count, results.Count);
            foreach (KahunaSetKeyValueResponseItem r in results)
                Assert.Equal(KeyValueResponseType.Set, r.Type);

            for (int i = 0; i < count; i++)
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, $"rev/k{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.NotNull(entry);
                // Exactly one revision per round: round 0 → 0, round 1 → 1, round 2 → 2. Any double-apply
                // would overshoot.
                Assert.Equal(round, entry!.Revision);
                Assert.Equal($"round{round}-v{i}", Encoding.UTF8.GetString(entry.Value!));
            }
        }
    }

    /// <summary>
    /// Keys drawn from several key spaces route to different partitions; the batched path must group by
    /// partition and propose each group once, writing every key correctly regardless of how the batch splits.
    /// </summary>
    [Fact]
    public async Task BatchedSetMany_AcrossKeySpaces_WritesEveryKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;

        List<KahunaSetKeyValueRequestItem> items = [];
        List<string> keys = [];
        for (int space = 0; space < 6; space++)
            for (int i = 0; i < 8; i++)
            {
                string key = $"space{space}/k{i}";
                keys.Add(key);
                items.Add(SetItem(key, Encoding.UTF8.GetBytes(key)));
            }

        List<KahunaSetKeyValueResponseItem> results = await kv.LocateAndTrySetManyKeyValue(items, ct);

        Assert.Equal(keys.Count, results.Count);
        foreach (KahunaSetKeyValueResponseItem r in results)
            Assert.Equal(KeyValueResponseType.Set, r.Type);

        foreach (string key in keys)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(entry);
            Assert.Equal(key, Encoding.UTF8.GetString(entry!.Value!));
        }
    }

    /// <summary>
    /// A conditional set (SetIfNotExists) inside a batch that fails its predicate must report NotSet for that
    /// key while the other keys still commit — the staging terminal path leaves nothing pinned.
    /// </summary>
    [Fact]
    public async Task BatchedSetMany_FailedConditional_ReportsNotSetWithoutBlockingOthers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;

        // Seed one key so a later SetIfNotExists on it fails.
        (KeyValueResponseType seed, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "cond/existing", Encoding.UTF8.GetBytes("orig"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, seed);

        List<KahunaSetKeyValueRequestItem> items =
        [
            new() { Key = "cond/existing", Value = Encoding.UTF8.GetBytes("new"), Flags = KeyValueFlags.SetIfNotExists, Durability = KeyValueDurability.Persistent },
            SetItem("cond/fresh1", Encoding.UTF8.GetBytes("f1")),
            SetItem("cond/fresh2", Encoding.UTF8.GetBytes("f2")),
        ];

        List<KahunaSetKeyValueResponseItem> results = await kv.LocateAndTrySetManyKeyValue(items, ct);

        Assert.Equal(KeyValueResponseType.NotSet, results.Single(r => r.Key == "cond/existing").Type);
        Assert.Equal(KeyValueResponseType.Set, results.Single(r => r.Key == "cond/fresh1").Type);
        Assert.Equal(KeyValueResponseType.Set, results.Single(r => r.Key == "cond/fresh2").Type);

        // The existing key keeps its original value; the fresh keys committed.
        (_, ReadOnlyKeyValueEntry? existing) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "cond/existing", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal("orig", Encoding.UTF8.GetString(existing!.Value!));

        (_, ReadOnlyKeyValueEntry? fresh1) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "cond/fresh1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal("f1", Encoding.UTF8.GetString(fresh1!.Value!));
    }

    // ── batched delete-many ──────────────────────────────────────────────────

    /// <summary>
    /// A batched delete-many over previously-set keys reports Deleted for each, and each key then reads back
    /// as gone. Keys are drawn from several key spaces so the batch also splits across partitions.
    /// </summary>
    [Fact]
    public async Task BatchedDeleteMany_DeletesSeededKeys_ThenGone()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;

        List<string> keys = [];
        List<KahunaSetKeyValueRequestItem> setItems = [];
        for (int space = 0; space < 5; space++)
            for (int i = 0; i < 8; i++)
            {
                string key = $"del{space}/k{i}";
                keys.Add(key);
                setItems.Add(SetItem(key, Encoding.UTF8.GetBytes(key)));
            }

        List<KahunaSetKeyValueResponseItem> setResults = await kv.LocateAndTrySetManyKeyValue(setItems, ct);
        Assert.All(setResults, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

        List<KahunaDeleteKeyValueRequestItem> delItems =
            keys.Select(k => new KahunaDeleteKeyValueRequestItem { Key = k, Durability = KeyValueDurability.Persistent }).ToList();

        List<KahunaDeleteKeyValueResponseItem> delResults = await kv.LocateAndTryDeleteManyKeyValue(delItems, ct);

        Assert.Equal(keys.Count, delResults.Count);
        Assert.All(delResults, r => Assert.Equal(KeyValueResponseType.Deleted, r.Type));

        foreach (string key in keys)
        {
            (KeyValueResponseType type, _) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.NotEqual(KeyValueResponseType.Get, type);
        }
    }

    /// <summary>
    /// A batched delete-many of keys that were never set reports DoesNotExist per key (a miss, not an error)
    /// and installs no lingering state — the staging terminal path.
    /// </summary>
    [Fact]
    public async Task BatchedDeleteMany_MissingKeys_ReportsDoesNotExist()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;

        List<KahunaDeleteKeyValueRequestItem> delItems = [];
        for (int i = 0; i < 20; i++)
            delItems.Add(new KahunaDeleteKeyValueRequestItem { Key = $"ghost/k{i}", Durability = KeyValueDurability.Persistent });

        List<KahunaDeleteKeyValueResponseItem> delResults = await kv.LocateAndTryDeleteManyKeyValue(delItems, ct);

        Assert.Equal(20, delResults.Count);
        Assert.All(delResults, r => Assert.Equal(KeyValueResponseType.DoesNotExist, r.Type));
    }

    /// <summary>
    /// A batch mixing present and absent keys deletes the present ones (Deleted) and reports the absent ones
    /// (DoesNotExist), each independently — the batch is not aborted by the missing keys.
    /// </summary>
    [Fact]
    public async Task BatchedDeleteMany_MixedPresentAndAbsent_ReportsEachIndependently()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;

        List<KahunaSetKeyValueRequestItem> setItems =
        [
            SetItem("mix/present0", Encoding.UTF8.GetBytes("a")),
            SetItem("mix/present1", Encoding.UTF8.GetBytes("b")),
        ];
        await kv.LocateAndTrySetManyKeyValue(setItems, ct);

        List<KahunaDeleteKeyValueRequestItem> delItems =
        [
            new() { Key = "mix/present0", Durability = KeyValueDurability.Persistent },
            new() { Key = "mix/absent0", Durability = KeyValueDurability.Persistent },
            new() { Key = "mix/present1", Durability = KeyValueDurability.Persistent },
        ];

        List<KahunaDeleteKeyValueResponseItem> delResults = await kv.LocateAndTryDeleteManyKeyValue(delItems, ct);

        Assert.Equal(KeyValueResponseType.Deleted, delResults.Single(r => r.Key == "mix/present0").Type);
        Assert.Equal(KeyValueResponseType.Deleted, delResults.Single(r => r.Key == "mix/present1").Type);
        Assert.Equal(KeyValueResponseType.DoesNotExist, delResults.Single(r => r.Key == "mix/absent0").Type);
    }
}

using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A cluster-wide backup under a replication factor is composed from one key-filtered as-of checkpoint per
/// node. These cases run the real backends: a filtered image holds only the accepted keys (with their history)
/// and no durability floor, and two filtered images of disjoint key sets merge into one image that holds both.
/// </summary>
public sealed class TestCheckpointComposition : IDisposable
{
    private const string Revision = "v1";

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_ckpt_compose_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private string NewDir(string name)
    {
        string dir = Path.Combine(_tempRoot, name + "_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static IPersistenceBackend Open(string storage, string path) => storage switch
    {
        "rocksdb" => new RocksDbPersistenceBackend(path, Revision),
        "sqlite" => new SqlitePersistenceBackend(path, Revision),
        _ => MemoryPersistenceBackend.OpenCheckpoint(path)
    };

    private static IPersistenceBackend NewStore(string storage, string path) => storage switch
    {
        "rocksdb" => new RocksDbPersistenceBackend(path, Revision),
        "sqlite" => new SqlitePersistenceBackend(path, Revision),
        _ => new MemoryPersistenceBackend()
    };

    // Where a backend opens a checkpoint directory: RocksDB under its revision, the others in place.
    private static string BackendRoot(string storage, string checkpointDir, string scratch)
    {
        if (storage != "rocksdb")
            return checkpointDir;

        string root = Path.Combine(scratch, "open_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(root);
        CopyTree(checkpointDir, Path.Combine(root, Revision));
        return root;
    }

    private static void CopyTree(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.GetFiles(source))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
        foreach (string dir in Directory.GetDirectories(source))
            CopyTree(dir, Path.Combine(destination, Path.GetFileName(dir)));
    }

    private static PersistenceRequestItem Item(string key, string value, long revision, long timeMs) =>
        new(key, Encoding.UTF8.GetBytes(value), revision,
            0, 0, 0, 0, 0, 0,
            0, timeMs, 0, (int)KeyValueState.Set);

    private static string Value(IPersistenceBackend backend, string key) =>
        Encoding.UTF8.GetString(backend.GetKeyValue(key)?.Value ?? []);

    private static bool OwnedByA(string key) => key.StartsWith("a/", StringComparison.Ordinal);

    [Theory]
    [InlineData("memory")]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public void FilteredImages_HoldOnlyTheirKeys_AndMergeIntoOneImageThatHoldsBoth(string storage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storePath = NewDir("store");
        string imageA = Path.Combine(NewDir("image"), "a");
        string imageB = Path.Combine(NewDir("image"), "b");
        HLCTimestamp cut = new(0, 1_000, 0);

        IPersistenceBackend store = NewStore(storage, storePath);
        try
        {
            Assert.True(store.SupportsFilteredCheckpoint);

            // a/k has two revisions at or before the cut and one after it; b/k one before the cut.
            Assert.True(store.StoreKeyValues([Item("a/k", "a-1", 1, 100)]));
            Assert.True(store.StoreKeyValues([Item("a/k", "a-2", 2, 200)]));
            Assert.True(store.StoreKeyValues([Item("a/k", "a-after-cut", 3, 5_000)]));
            Assert.True(store.StoreKeyValues([Item("b/k", "b-1", 1, 300)]));
            Assert.True(store.StoreKeyValues([Item("a/only", "a-only", 1, 400)]));
            store.StoreDurabilityFloors([(1, 42)]);

            store.CreateCheckpointAsOf(imageA, 10, cut, OwnedByA, ct);
            store.CreateCheckpointAsOf(imageB, 12, cut, key => !OwnedByA(key), ct);
        }
        finally
        {
            (store as IDisposable)?.Dispose();
        }

        string scratch = NewDir("scratch");

        IPersistenceBackend onlyA = Open(storage, BackendRoot(storage, imageA, scratch));
        try
        {
            Assert.Equal("a-2", Value(onlyA, "a/k"));
            Assert.Equal("a-only", Value(onlyA, "a/only"));
            Assert.Null(onlyA.GetKeyValue("b/k"));
            if (storage != "memory")
            {
                // The history at or before the cut stays with the key; the floor certified the old WAL only.
                Assert.NotNull(onlyA.GetKeyValueRevision("a/k", 1));
                Assert.Equal(-1, onlyA.GetDurabilityFloor(1));
            }
        }
        finally
        {
            (onlyA as IDisposable)?.Dispose();
        }

        CheckpointComposer.MergeInto(storage, Revision, imageA, imageB, ct);

        IPersistenceBackend merged = Open(storage, BackendRoot(storage, imageA, scratch));
        try
        {
            Assert.Equal("a-2", Value(merged, "a/k"));
            Assert.Equal("a-only", Value(merged, "a/only"));
            Assert.Equal("b-1", Value(merged, "b/k"));
            if (storage != "memory")
                Assert.NotNull(merged.GetKeyValueRevision("a/k", 1));
        }
        finally
        {
            (merged as IDisposable)?.Dispose();
        }

        // The sidecar records the larger of the two images' indexes.
        Assert.Equal(12, CheckpointManifest.ReadFrom(imageA).AppliedIndex);
    }

    [Fact]
    public void SqliteMerge_AdoptsTheTargetRevision_ForAShareWrittenUnderAnother()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourcePath = NewDir("other_revision");
        string image = Path.Combine(NewDir("image"), "other");
        string target = Path.Combine(NewDir("image"), "target");
        HLCTimestamp cut = new(0, 1_000, 0);

        using (SqlitePersistenceBackend other = new(sourcePath, "other-rev"))
        {
            Assert.True(other.StoreKeyValues([Item("b/k", "b-1", 1, 300)]));
            other.CreateCheckpointAsOf(image, 3, cut, _ => true, ct);
        }

        using (SqlitePersistenceBackend local = new(NewDir("local"), Revision))
        {
            Assert.True(local.StoreKeyValues([Item("a/k", "a-1", 1, 300)]));
            local.CreateCheckpointAsOf(target, 4, cut, _ => true, ct);
        }

        CheckpointComposer.MergeInto("sqlite", Revision, target, image, ct);

        using SqlitePersistenceBackend merged = new(target, Revision);
        Assert.Equal("a-1", Value(merged, "a/k"));
        Assert.Equal("b-1", Value(merged, "b/k"));
        Assert.Empty(Directory.GetFiles(target, "*other-rev*"));
    }

    private static PreparedIntent Intent(HLCTimestamp tx, string key) => new(
        tx, 1, key, 0, key, new HLCTimestamp(1, 600, 0), KeyValueState.Set, Encoding.UTF8.GetBytes("v"), null, 2,
        HLCTimestamp.Zero, false, 1, KeyValueState.Set, HLCTimestamp.Zero, PreparedIntentResolution.Pending);

    private static IntentFrontier Frontier(IEnumerable<PreparedIntent> live, params (HLCTimestamp, long, string)[] settled)
    {
        using MemoryStream stream = new(PreparedIntentStore.SerializeBackupFrontier(live, settled));
        return IntentFrontier.Deserialize(stream);
    }

    [Fact]
    public void FrontierMerge_KeepsEveryLiveIntent_AndDropsASettledIdentityAnotherShareHoldsLive()
    {
        HLCTimestamp tx = new(1, 500, 0);
        HLCTimestamp other = new(2, 510, 0);

        // Share A holds a/k live; share B saw the same transaction's identity settled (a cross-partition
        // transaction can settle on one partition first) and holds b/k live with another settled identity.
        IntentFrontier a = Frontier([Intent(tx, "a/k")]);
        IntentFrontier b = Frontier([Intent(other, "b/k")], (tx, 1, "a/k"), (other, 1, "b/old"));

        IntentFrontier merged = IntentFrontier.Merge([a, b, IntentFrontier.Empty]);

        Assert.Equal(["a/k", "b/k"], merged.Live.Select(i => i.Key).Order());
        Assert.DoesNotContain(new PreparedIntentIdentity(tx, 1, "a/k"), merged.Settled);
        Assert.Contains(new PreparedIntentIdentity(other, 1, "b/old"), merged.Settled);
    }
}

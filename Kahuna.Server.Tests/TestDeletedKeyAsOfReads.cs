
using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// As-of (snapshot) reads of deleted keys. A delete commits its tombstone as a first-class
/// revision (live revision + 1), so the last live value keeps its own persisted revision record
/// and stays readable at any snapshot timestamp before the delete — while resident, after the
/// background flush, and after the pre-delete revision has been trimmed from the in-memory
/// archive (served from the persisted revision history). A tombstone that reused the live
/// revision number would overwrite that record on flush, silently destroying the key's history.
/// </summary>
public sealed class TestDeletedKeyAsOfReads
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeletedKeyAsOfReads(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string S(byte[]? b) => b is null ? "" : Encoding.UTF8.GetString(b);

    private static async Task<EmbeddedKahunaNode> StartNode(
        ILoggerFactory loggerFactory, CancellationToken ct, int revisionRetention = 16)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            RevisionRetention = revisionRetention,
            DirtyObjectsWriterDelay = 100
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("asofdel", ct);
        return node;
    }

    /// <summary>
    /// Non-transactional path: the tombstone allocates a new revision, the pre-delete snapshot
    /// stays readable before and after the flush, a snapshot at the delete itself reads as
    /// absent, and a delete→re-set cycle keeps revisions strictly monotonic.
    /// </summary>
    [Fact]
    public async Task Delete_TombstoneTakesNewRevision_PreDeleteSnapshotStaysReadable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string key = "asofdel/point/" + Guid.NewGuid().ToString("N")[..8];

        (KeyValueResponseType st, long setRevision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, B("v1"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, st);

        (_, ReadOnlyKeyValueEntry? liveEntry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.NotNull(liveEntry);
        HLCTimestamp snapshotT = liveEntry!.LastModified;

        await Task.Delay(2, ct); // keep the delete's HLC strictly after snapshotT

        (KeyValueResponseType dt, long deleteRevision, HLCTimestamp deleteT) =
            await kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Deleted, dt);

        // The tombstone is its own revision — it must not reuse (and later overwrite) the live one.
        Assert.Equal(setRevision + 1, deleteRevision);

        // Pre-delete snapshot reads while the entry (and its revision archive) is resident.
        (KeyValueResponseType rt, ReadOnlyKeyValueEntry? snap) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rt);
        Assert.Equal("v1", S(snap!.Value));
        Assert.Equal(setRevision, snap.Revision);

        (KeyValueResponseType et, ReadOnlyKeyValueEntry? existsEntry) = await kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Exists, et);
        Assert.Equal(setRevision, existsEntry!.Revision);

        // A snapshot at the delete's own timestamp observes the tombstone.
        (KeyValueResponseType atDel, _) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, deleteT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, atDel);

        // After the flush both revision records are on disk; the answers must not change.
        await kahuna.FlushPersistenceAsync();

        (rt, snap) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rt);
        Assert.Equal("v1", S(snap!.Value));

        (atDel, _) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, deleteT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, atDel);

        // Delete→re-set stays monotonic and keeps the whole history resolvable.
        await Task.Delay(2, ct);
        (st, long resetRevision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, B("v2"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, st);
        Assert.Equal(deleteRevision + 1, resetRevision);

        (KeyValueResponseType lt, ReadOnlyKeyValueEntry? latest) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, lt);
        Assert.Equal("v2", S(latest!.Value));

        (rt, snap) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rt);
        Assert.Equal("v1", S(snap!.Value));
    }

    /// <summary>
    /// The disk-history proof: after the pre-delete revision has been trimmed from the in-memory
    /// archive, the snapshot read must be served from the persisted revision history. This is
    /// exactly the record a same-revision tombstone used to overwrite on flush.
    /// </summary>
    [Fact]
    public async Task PreDeleteSnapshot_AfterTrim_ServedFromDiskHistory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct, revisionRetention: 2);
        IKahuna kahuna = node.Kahuna;

        string key = "asofdel/disk/" + Guid.NewGuid().ToString("N")[..8];

        (KeyValueResponseType st, long v1Revision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, B("v1"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, st);

        (_, ReadOnlyKeyValueEntry? liveEntry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        HLCTimestamp snapshotT = liveEntry!.LastModified;

        await Task.Delay(2, ct);

        (KeyValueResponseType dt, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Deleted, dt);

        // Supersede past RevisionRetention so both v1 and the tombstone leave the in-memory archive.
        for (int i = 2; i <= 6; i++)
        {
            await Task.Delay(2, ct);
            (st, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, B($"v{i}"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, st);
        }

        await kahuna.FlushPersistenceAsync();

        (KeyValueResponseType rt, ReadOnlyKeyValueEntry? snap) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rt);
        Assert.Equal("v1", S(snap!.Value));
        Assert.Equal(v1Revision, snap.Revision);

        (KeyValueResponseType et, ReadOnlyKeyValueEntry? existsEntry) = await kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Exists, et);
        Assert.Equal(v1Revision, existsEntry!.Revision);
    }

    /// <summary>
    /// Bucket and range scans at a pre-delete snapshot must include the deleted key with its
    /// pre-delete value. Their snapshot projection resolves at-or-before (head revision - 1),
    /// which lands on the live value only because the tombstone occupies its own revision slot.
    /// </summary>
    [Fact]
    public async Task BucketAndRangeScans_PreDeleteSnapshot_IncludeDeletedKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string bucket = "asofdelscan" + Guid.NewGuid().ToString("N")[..8];
        string deletedKey = bucket + "/k1";
        string survivorKey = bucket + "/k2";

        foreach (string key in new[] { deletedKey, survivorKey })
        {
            (KeyValueResponseType st, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, B("v1"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, st);
        }

        // Snapshot timestamp at-or-after both writes and strictly before the delete.
        HLCTimestamp snapshotT = HLCTimestamp.Zero;
        foreach (string key in new[] { deletedKey, survivorKey })
        {
            (_, ReadOnlyKeyValueEntry? e) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            if (e!.LastModified.CompareTo(snapshotT) > 0)
                snapshotT = e.LastModified;
        }

        await Task.Delay(2, ct);

        (KeyValueResponseType dt, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, deletedKey, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Deleted, dt);

        // Land both revision records on disk: the scans' snapshot projection reads the persisted history.
        await kahuna.FlushPersistenceAsync();

        KeyValueGetByBucketResult bucketAtSnapshot = await kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero, bucket, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, bucketAtSnapshot.Type);
        Assert.Equal(
            new[] { deletedKey, survivorKey },
            bucketAtSnapshot.Items.Select(i => i.Item1).Order(StringComparer.Ordinal).ToArray());
        Assert.Equal("v1", S(bucketAtSnapshot.Items.Single(i => i.Item1 == deletedKey).Item2.Value));

        KeyValueGetByRangeResult rangeAtSnapshot = await kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 100, snapshotT,
            KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rangeAtSnapshot.Type);
        Assert.Contains(rangeAtSnapshot.Items, i => i.Item1 == deletedKey && S(i.Item2.Value) == "v1");

        // A latest (non-snapshot) scan honors the delete.
        KeyValueGetByBucketResult bucketLatest = await kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero, bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, bucketLatest.Type);
        Assert.Equal([survivorKey], bucketLatest.Items.Select(i => i.Item1).ToArray());
    }

    /// <summary>
    /// Transactional (script) path: a delete staged under an open transaction takes a new
    /// revision through the MVCC entry, so pre-delete snapshots of a transactionally deleted
    /// key stay readable before and after the flush.
    /// </summary>
    [Fact]
    public async Task ScriptDelete_PreDeleteSnapshotStaysReadable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string key = "asofdel/tx/" + Guid.NewGuid().ToString("N")[..8];

        KeyValueTransactionResult set = await RunScript(node, $"BEGIN SET `{key}` 'v1' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, set.Type);

        (_, ReadOnlyKeyValueEntry? liveEntry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.NotNull(liveEntry);
        HLCTimestamp snapshotT = liveEntry!.LastModified;
        long liveRevision = liveEntry.Revision;

        await Task.Delay(2, ct);

        KeyValueTransactionResult del = await RunScript(node, $"BEGIN DELETE `{key}` COMMIT END");
        Assert.Equal(KeyValueResponseType.Deleted, del.Type);

        (KeyValueResponseType lt, _) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, lt);

        (KeyValueResponseType rt, ReadOnlyKeyValueEntry? snap) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rt);
        Assert.Equal("v1", S(snap!.Value));
        Assert.Equal(liveRevision, snap.Revision);

        await kahuna.FlushPersistenceAsync();

        (rt, snap) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rt);
        Assert.Equal("v1", S(snap!.Value));
        Assert.Equal(liveRevision, snap.Revision);
    }

    /// <summary>A durable transaction touching a key with a lingering committed-but-unsettled
    /// intent can transiently answer MustRetry; the operation lands on retry.</summary>
    private static async Task<KeyValueTransactionResult> RunScript(EmbeddedKahunaNode node, string script)
    {
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        for (int attempt = 1; result.Type == KeyValueResponseType.MustRetry && attempt < 40; attempt++)
        {
            await Task.Delay(Math.Min(5 * attempt, 50));
            result = await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        }

        return result;
    }
}

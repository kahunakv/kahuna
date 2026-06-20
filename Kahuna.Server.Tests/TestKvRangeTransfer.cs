using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the KV state-machine transfer primitive: export a key range at a fixed MVCC
/// snapshot and import it atomically into a fresh store. Uses in-process single-node
/// EmbeddedKahunaNodes (memory storage), <b>one at a time</b> — the export stream is captured as a
/// byte buffer and the source disposed before the target starts, because two embedded nodes share
/// static witness state and cannot run concurrently.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestKvRangeTransfer
{
    private readonly ILoggerFactory loggerFactory;

    public TestKvRangeTransfer(ITestOutputHelper outputHelper)
    {
        loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
    }

    private static EmbeddedKahunaOptions MemoryOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    };

    private static KvStateMachineTransfer Transfer(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).KvStateMachineTransfer;

    private static async Task SeedKeys(EmbeddedKahunaNode node, string prefix, int from, int count)
    {
        for (int i = from; i < from + count; i++)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                $"{prefix}/{i:D4}",
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, type);
        }
    }

    private static async Task<List<(string Key, ReadOnlyKeyValueEntry Entry)>> ReadRange(
        EmbeddedKahunaNode node, string prefix)
    {
        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            10_000, HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        return result.Items.Select(static i => (i.Item1, i.Item2)).ToList();
    }

    private static HLCTimestamp Now(EmbeddedKahunaNode node) =>
        node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

    /// <summary>Parses an export stream into its entries (the authoritative source snapshot, key-sorted).</summary>
    private static List<RangeSnapshotEntry> ParseSnapshot(byte[] bytes)
    {
        List<RangeSnapshotEntry> entries = [];
        using MemoryStream ms = new(bytes);
        while (true)
        {
            RangeSnapshotPage page = RangeSnapshotPage.Parser.ParseDelimitedFrom(ms);
            entries.AddRange(page.Entries);
            if (!page.HasMore)
                break;
        }
        return entries;
    }

    /// <summary>Seeds a source node, exports [prefix] at a snapshot, returns the raw stream bytes; source disposed.</summary>
    private async Task<byte[]> SeedAndExport(string prefix, int count, Action<EmbeddedKahunaNode, HLCTimestamp>? afterSnapshot = null)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode source = new(MemoryOptions(), loggerFactory);
        await source.StartAsync(ct);

        await SeedKeys(source, prefix, 0, count);
        HLCTimestamp snapshotTs = Now(source);
        afterSnapshot?.Invoke(source, snapshotTs);

        await using Stream snapshot = await Transfer(source).ExportRangeAsync(
            prefix, null, null, snapshotTs, KeyValueDurability.Persistent, ct);

        return ((MemoryStream)snapshot).ToArray();
    }

    // ── ExportImport_RoundTripsByteIdentical ─────────────────────────────────────

    [Fact]
    public async Task ExportImport_RoundTripsByteIdentical()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        byte[] exported = await SeedAndExport("t:r", 25);
        List<RangeSnapshotEntry> expected = ParseSnapshot(exported);
        Assert.Equal(25, expected.Count);

        await using EmbeddedKahunaNode target = new(MemoryOptions(), loggerFactory);
        await target.StartAsync(ct);

        await Transfer(target).ImportRangeAsync(new MemoryStream(exported), ct);

        List<(string Key, ReadOnlyKeyValueEntry Entry)> actual = await ReadRange(target, "t:r");
        Assert.Equal(expected.Count, actual.Count);

        for (int i = 0; i < expected.Count; i++)
        {
            RangeSnapshotEntry e = expected[i];
            ReadOnlyKeyValueEntry a = actual[i].Entry;

            Assert.Equal(e.Key, actual[i].Key);
            Assert.Equal(e.HasValue ? e.Value.ToByteArray() : null, a.Value);   // byte-identical value
            Assert.Equal(e.Revision, a.Revision);
            Assert.Equal(e.ExpiresNode, a.Expires.N);                           // HLC exact (component-wise)
            Assert.Equal(e.ExpiresPhysical, a.Expires.L);
            Assert.Equal(e.ExpiresCounter, a.Expires.C);
            Assert.Equal(e.LastModifiedNode, a.LastModified.N);
            Assert.Equal(e.LastModifiedPhysical, a.LastModified.L);
            Assert.Equal(e.LastModifiedCounter, a.LastModified.C);
            Assert.Equal(e.State, (int)a.State);
        }
    }

    // ── Import_AtomicOnCrash ─────────────────────────────────────────────────────

    [Fact]
    public async Task Import_AtomicOnCrash()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        byte[] exported = await SeedAndExport("t:r", 25);
        Assert.True(exported.Length > 16);

        await using EmbeddedKahunaNode target = new(MemoryOptions(), loggerFactory);
        await target.StartAsync(ct);

        // Reads fail partway → a crash during the read/buffer phase, before any apply. The import
        // applies nothing until the whole stream is buffered+verified, so this must leave the target
        // empty. (Note: the StoreKeyValues apply itself commits per shard and is NOT cross-shard
        // atomic; a crash mid-store is handled by idempotent re-import + cutover-after-success in
        // the split transaction, not by this primitive — see KvStateMachineTransfer remarks.)
        await using ThrowingStream crashing = new(exported, failAfter: exported.Length / 2);

        await Assert.ThrowsAnyAsync<Exception>(
            async () => await Transfer(target).ImportRangeAsync(crashing, ct));

        Assert.Empty(await ReadRange(target, "t:r"));
    }

    // ── Import_TruncatedStream_FailsCleanly ──────────────────────────────────────

    [Fact]
    public async Task Import_TruncatedStream_FailsCleanly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        byte[] exported = await SeedAndExport("t:r", 25);
        byte[] truncated = exported[..(exported.Length / 2)]; // cut before the terminal sentinel

        await using EmbeddedKahunaNode target = new(MemoryOptions(), loggerFactory);
        await target.StartAsync(ct);

        // A truncated stream fails with a clear error (not an NRE), and applies nothing.
        await Assert.ThrowsAsync<KahunaServerException>(
            async () => await Transfer(target).ImportRangeAsync(new MemoryStream(truncated), ct));

        Assert.Empty(await ReadRange(target, "t:r"));
    }

    // ── Export_RespectsUpToIndex ─────────────────────────────────────────────────

    [Fact]
    public async Task Export_RespectsUpToIndex()
    {
        // Snapshot captured after 10 keys; 10 more written *after* it must not appear in the export.
        byte[] exported = await SeedAndExport("t:r", 10, afterSnapshot: (source, _) =>
            SeedKeys(source, "t:r", 10, 10).GetAwaiter().GetResult());

        List<RangeSnapshotEntry> entries = ParseSnapshot(exported);

        Assert.Equal(10, entries.Count);
        Assert.All(entries, e => Assert.True(string.CompareOrdinal(e.Key, "t:r/0010") < 0, e.Key));
    }

    /// <summary>A read-only stream over a buffer that throws once a byte threshold is crossed.</summary>
    private sealed class ThrowingStream(byte[] buffer, int failAfter) : Stream
    {
        private int position;

        public override int Read(byte[] target, int offset, int count)
        {
            if (position >= failAfter)
                throw new IOException("injected mid-import crash");

            int n = Math.Min(count, Math.Min(failAfter, buffer.Length) - position);
            Array.Copy(buffer, position, target, offset, n);
            position += n;
            return n;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => buffer.Length;
        public override long Position { get => position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
        public override void SetLength(long v) => throw new NotSupportedException();
        public override void Write(byte[] b, int o, int c) => throw new NotSupportedException();
    }
}

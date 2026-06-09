using Google.Protobuf;
using Kommander;
using Kommander.System;
using Kommander.Time;

using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The key-range data-movement primitive: export a key range's KV state at a
/// fixed MVCC snapshot to a stream, and import such a stream into a target store atomically. Task 6
/// (the split transaction) uses this to move <c>[K, E)</c> from a partition to a freshly-created one.
///
/// <para>
/// <b>Why a Kahuna-native API alongside <see cref="IRaftStateMachineTransfer"/>.</b> Kommander's
/// <see cref="RaftSplitPlan"/> carries only a hash boundary — <b>no key range</b> — so the interface
/// <see cref="ExportRange(RaftSplitPlan,long,System.Threading.CancellationToken)"/> cannot express
/// <c>[K, E)</c>. Kahuna therefore drives key-range transfers through
/// <see cref="ExportRangeAsync"/> / <see cref="ImportRangeAsync"/> with explicit bounds (it does not
/// call <c>SplitPartitionAsync</c> for data; design §7). The instance is still registered with
/// Kommander so any coordinator-driven <i>import</i> works; the plan-based <i>export</i> throws
/// (it is never invoked by Kahuna's key-range path).
/// </para>
///
/// <para>
/// <b>Data model.</b> The persistence backend is node-global and keyed by key string, so a transfer
/// is fundamentally: read <c>[startKey, endKey)</c> at a fixed snapshot via the existing
/// <see cref="KeyValuesManager.GetByRange"/> MVCC paging → serialize bounded, checksummed pages →
/// apply via <see cref="IPersistenceBackend.StoreKeyValues"/>.
/// </para>
///
/// <para>
/// <b>Import safety (idempotent-copy model, design §5.4 — <i>not</i> raw all-or-nothing).</b> Entries
/// are buffered and applied only after the whole stream is read + checksum-verified, so a crash while
/// <i>reading</i> is a clean no-op. The apply itself is <b>not</b> cross-shard atomic:
/// <see cref="IPersistenceBackend.StoreKeyValues"/> groups by shard and commits one transaction per
/// shard, so a crash <i>mid-store</i> can leave a partial apply. That is safe by construction — the
/// target is a fresh empty partition, the copy is idempotent (re-import overwrites the same keys), and
/// the split's atomic point is the cutover meta-transaction (Task 6), which runs <b>only after a
/// fully-successful import</b>. A partial/failed import is retried or the fresh partition discarded; it
/// is never cut over.
/// </para>
/// </summary>
internal sealed class KvStateMachineTransfer : IRaftStateMachineTransfer
{
    /// <summary>Entries per exported page (bounded memory + checksum granularity).</summary>
    private const int PageSize = 256;

    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    private readonly KeyValuesManager manager;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly ILogger<IKahuna> logger;

    public KvStateMachineTransfer(
        KeyValuesManager manager,
        IPersistenceBackend persistenceBackend,
        ILogger<IKahuna> logger)
    {
        this.manager = manager;
        this.persistenceBackend = persistenceBackend;
        this.logger = logger;
    }

    /// <summary>
    /// Exports <c>[startKey, endKey)</c> within <paramref name="keySpacePrefix"/> as a readable stream
    /// of bounded, checksummed <see cref="RangeSnapshotPage"/>s, reflecting exactly the committed state
    /// at <paramref name="snapshotTs"/> (MVCC: entries modified after it are excluded). Null bounds
    /// mean the whole key space. The returned stream is positioned at 0.
    /// </summary>
    public async Task<Stream> ExportRangeAsync(
        string keySpacePrefix,
        string? startKey,
        string? endKey,
        HLCTimestamp snapshotTs,
        KeyValueDurability durability,
        CancellationToken ct)
    {
        // Persistent only. Export reads the memory+disk merge (so it sees the latest committed
        // writes), but import writes the backend only and does not warm the target's in-memory actor
        // store — persistent reads fall through to the backend, so that asymmetry is correct for
        // committed row/index data. Ephemeral entries live *only* in the in-memory store and would be
        // lost on import, so they are out of scope for key-range transfers.
        if (durability == KeyValueDurability.Ephemeral)
            throw new NotSupportedException(
                "Range transfer supports persistent key spaces only; ephemeral data is in-memory-only and is not transferable.");

        MemoryStream stream = new();

        string? cursorKey = startKey;
        bool cursorInclusive = true;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            KeyValueGetByRangeResult page = await manager.GetByRange(
                HLCTimestamp.Zero,
                keySpacePrefix,
                cursorKey, cursorInclusive,
                endKey, false,
                PageSize,
                snapshotTs,
                durability).ConfigureAwait(false);

            if (page.Type != KeyValueResponseType.Get && page.Items.Count == 0)
            {
                // No data (empty range or not-yet-ready): emit a single terminal empty page so the
                // importer always sees a hasMore=false sentinel and never has to detect EOF.
                WritePage(stream, [], hasMore: false);
                break;
            }

            bool hasMore = page.HasMore && page.Items.Count > 0;
            WritePage(stream, page.Items, hasMore);

            if (!hasMore)
                break;

            cursorKey = page.Items[^1].Item1;   // resume strictly after the last key
            cursorInclusive = false;
        }

        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Installs an exported snapshot into this node's KV store. Reads + checksum-verifies every page,
    /// buffers all entries, then applies them via <see cref="IPersistenceBackend.StoreKeyValues"/>. A
    /// crash/cancel before the apply leaves the target untouched; the apply commits per shard (not
    /// cross-shard atomic), so a crash mid-store may leave a partial apply — safe because the copy is
    /// idempotent and Task 6 cuts over only after a fully-successful import (see type remarks, §5.4).
    /// </summary>
    public Task ImportRangeAsync(Stream snapshot, CancellationToken ct)
    {
        List<PersistenceRequestItem> items = [];

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            RangeSnapshotPage? page;
            try
            {
                page = RangeSnapshotPage.Parser.ParseDelimitedFrom(snapshot);
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new KahunaServerException($"ImportRange: truncated or corrupt snapshot stream — {ex.Message}");
            }

            // A well-formed stream always ends with a hasMore=false page, so we break before EOF.
            // Reaching EOF here (null) means the stream was truncated before the terminal sentinel.
            if (page is null)
                throw new KahunaServerException("ImportRange: truncated snapshot stream (no terminal page).");

            ulong expected = ChecksumOf(page.Entries);
            if (expected != page.Checksum)
                throw new KahunaServerException(
                    $"ImportRange: page checksum mismatch (expected {expected}, got {page.Checksum}) — corrupt snapshot.");

            foreach (RangeSnapshotEntry entry in page.Entries)
                items.Add(ToPersistenceItem(entry));

            if (!page.HasMore)
                break;
        }

        // Apply only after the whole stream is buffered + verified, so a crash while reading is a
        // no-op. StoreKeyValues commits per shard (not cross-shard atomic), so a crash mid-store can
        // leave a partial apply — safe by construction: the copy is idempotent and Task 6 cuts over
        // only after a fully-successful import (§5.4).
        ct.ThrowIfCancellationRequested();

        if (items.Count > 0 && !persistenceBackend.StoreKeyValues(items))
            throw new KahunaServerException("ImportRange: StoreKeyValues failed to persist the snapshot.");

        return Task.CompletedTask;
    }

    // ── IRaftStateMachineTransfer (Kommander-driven path) ────────────────────────

    /// <summary>
    /// Not supported: <see cref="RaftSplitPlan"/> carries no key range, so a plan-based export cannot
    /// express <c>[K, E)</c>. Kahuna drives key-range exports via <see cref="ExportRangeAsync"/>.
    /// </summary>
    public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
        throw new NotSupportedException(
            "Kahuna key-range transfers are driven via ExportRangeAsync with explicit key bounds; " +
            "RaftSplitPlan carries no key range (design §7).");

    /// <summary>Coordinator-driven import — applies the stream exactly like the native path.</summary>
    public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
        ImportRangeAsync(snapshot, ct);

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static void WritePage(Stream stream, List<(string, ReadOnlyKeyValueEntry)> items, bool hasMore)
    {
        RangeSnapshotPage page = new() { HasMore = hasMore };

        foreach ((string key, ReadOnlyKeyValueEntry entry) in items)
            page.Entries.Add(ToSnapshotEntry(key, entry));

        page.Checksum = ChecksumOf(page.Entries);
        page.WriteDelimitedTo(stream);
    }

    private static RangeSnapshotEntry ToSnapshotEntry(string key, ReadOnlyKeyValueEntry entry)
    {
        RangeSnapshotEntry message = new()
        {
            Key = key,
            Revision = entry.Revision,
            ExpiresNode = entry.Expires.N,
            ExpiresPhysical = entry.Expires.L,
            ExpiresCounter = entry.Expires.C,
            LastUsedNode = entry.LastUsed.N,
            LastUsedPhysical = entry.LastUsed.L,
            LastUsedCounter = entry.LastUsed.C,
            LastModifiedNode = entry.LastModified.N,
            LastModifiedPhysical = entry.LastModified.L,
            LastModifiedCounter = entry.LastModified.C,
            State = (int)entry.State
        };

        if (entry.Value is not null)
            message.Value = UnsafeByteOperations.UnsafeWrap(entry.Value);

        return message;
    }

    private static PersistenceRequestItem ToPersistenceItem(RangeSnapshotEntry entry)
    {
        byte[]? value = entry.HasValue ? entry.Value.ToByteArray() : null;

        return new PersistenceRequestItem(
            entry.Key,
            value,
            entry.Revision,
            entry.ExpiresNode, entry.ExpiresPhysical, entry.ExpiresCounter,
            entry.LastUsedNode, entry.LastUsedPhysical, entry.LastUsedCounter,
            entry.LastModifiedNode, entry.LastModifiedPhysical, entry.LastModifiedCounter,
            entry.State);
    }

    /// <summary>FNV-1a 64 over the serialized entries (order-sensitive), for per-page integrity.</summary>
    private static ulong ChecksumOf(IEnumerable<RangeSnapshotEntry> entries)
    {
        ulong hash = FnvOffsetBasis;

        using MemoryStream buffer = new();
        foreach (RangeSnapshotEntry entry in entries)
            entry.WriteTo(buffer);

        foreach (byte b in buffer.GetBuffer().AsSpan(0, (int)buffer.Length))
        {
            hash ^= b;
            hash *= FnvPrime;
        }

        return hash;
    }
}

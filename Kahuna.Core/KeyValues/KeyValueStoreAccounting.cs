namespace Kahuna.Server.KeyValues;

using Kommander.Time;

/// <summary>
/// Approximate in-memory footprint of a store entry for budget checks. Includes value bytes plus
/// per-entry revision and MVCC metadata so hot keys with large histories count toward
/// <see cref="Kahuna.Server.Configuration.KahunaConfiguration.MaxBytesPerActor"/>.
///
/// <para>Constants are calibrated against 64-bit .NET managed-object sizes (object header 16 bytes,
/// Dictionary backing arrays at minimum capacity). <see cref="DictionaryOverheadBytes"/> covers the
/// dictionary object (~96 bytes) plus its lazily-allocated initial backing arrays (_buckets int[4]
/// ~32 bytes, _entries array header ~16 bytes), charged once on first Add. Per-entry costs capture
/// the full slot in the _entries array (hashCode + next + key + value/ref) plus any heap object for
/// reference-typed values. Value bytes are charged 1:1 via the callers; they dominate in real workloads
/// and are intentionally excluded from these structural constants.</para>
///
/// <para>In-place metadata growth between trim cycles is reconciled when collection trims survivors;
/// value-only mutations use <see cref="KeyValueContext.AdjustEntryValueBytes"/>.</para>
/// </summary>
internal static class KeyValueStoreAccounting
{
    public const int EntryOverheadBytes = 152; // +24 for StoreKey ref + LruPrev ref + LruNext ref

    /// <summary>
    /// Charged once when the first entry is added to <c>entry.MvccEntries</c> (a
    /// <c>Dictionary&lt;HLCTimestamp, KeyValueMvccEntry&gt;</c>). Covers the Dictionary object
    /// itself (~96 bytes) plus its lazily-allocated initial backing arrays at minimum capacity:
    /// _buckets int[4] (~32 bytes) and the _entries array object header (~16 bytes) plus
    /// prime-capacity slack. Intentionally rounded up to err on the side of earlier eviction.
    /// </summary>
    internal const int DictionaryOverheadBytes = 192;

    /// <summary>
    /// Charged once when the first entry is added to <c>entry.Revisions</c>
    /// (<see cref="KeyValueRevisionHistory"/>): the object header (16 bytes) plus the
    /// <c>_items</c> ref (8 bytes) plus <c>_count</c> int (4 bytes) plus padding (4 bytes) = 32
    /// bytes for the object, plus the initial <c>(long, KeyValueRevisionEntry)[1]</c> array header
    /// (16 bytes) = 48 bytes. The single slot in the initial array is charged separately via
    /// <see cref="RevisionEntryOverheadBytes"/>. Per-entry charging counts only occupied slots, so a
    /// history caught mid-doubling under-counts its empty backing-array capacity by at most one slot
    /// per occupied slot; this is bounded (n ≤ 17 → &lt; ~1.1 KB) and dominated by value bytes.
    /// </summary>
    internal const int RevisionHistoryOverheadBytes = 48;

    /// <summary>
    /// Per-slot cost in a <see cref="KeyValueRevisionHistory"/> beyond the long key itself
    /// (charged separately as <c>sizeof(long)</c>). Each slot in the internal
    /// <c>(long Key, KeyValueRevisionEntry Value)[]</c> array is 72 bytes: 8 bytes for the long key
    /// plus the 64-byte inline <c>KeyValueRevisionEntry</c> struct
    /// (Value ref 8 + LastModified HLCTimestamp 24 + Expires HLCTimestamp 24 + State 4 + padding 4).
    /// We charge the 64 non-key bytes here; the 8-byte key is charged via <c>sizeof(long)</c> at
    /// call sites. The Value byte array's referent is charged separately via value.Length, but its
    /// 8-byte reference field is real slot memory and stays counted in the struct.
    /// (HLCTimestamp measures 24 bytes: int N + long L + uint C + padding.)
    /// </summary>
    internal const int RevisionEntryOverheadBytes = 64;

    /// <summary>
    /// Per-entry cost in a <c>Dictionary&lt;HLCTimestamp, KeyValueMvccEntry&gt;</c>: the slot in
    /// the _entries array (hashCode 4 + next 4 + key HLCTimestamp 24 + value-ref 8 = 40 bytes)
    /// plus the <c>KeyValueMvccEntry</c> heap object (measured 112 bytes: header 16 + Value-ref 8 +
    /// Expires 24 + Revision 8 + LastUsed 24 + LastModified 24 + State 4 + NoRevision 1 + padding),
    /// for 152 bytes; rounded up to 160 to bias toward earlier eviction. The Value byte array itself
    /// is charged separately via snapshotValue.Length. (HLCTimestamp measures 24 bytes.)
    /// </summary>
    private const int MvccEntryOverheadBytes = 160;

    /// <summary>
    /// Bytes charged when a new revision entry is archived into entry.Revisions.
    /// Pass historyJustCreated=true when entry.Revisions was null before this call, so the
    /// one-time <see cref="RevisionHistoryOverheadBytes"/> for the container object is charged.
    /// </summary>
    internal static long EstimateRevisionAddedBytes(bool historyJustCreated, byte[]? archivedValue)
    {
        long bytes = sizeof(long) + RevisionEntryOverheadBytes + (archivedValue?.Length ?? 0);
        if (historyJustCreated)
            bytes += RevisionHistoryOverheadBytes;
        return bytes;
    }

    /// <summary>
    /// Bytes to reclaim when a single revision entry is removed from entry.Revisions.
    /// Pass historyNowEmpty=true when the history is empty after this removal,
    /// so the one-time <see cref="RevisionHistoryOverheadBytes"/> charged on first add is also reclaimed.
    /// </summary>
    internal static long EstimateRevisionRemovedBytes(bool historyNowEmpty, byte[]? archivedValue)
    {
        long bytes = sizeof(long) + RevisionEntryOverheadBytes + (archivedValue?.Length ?? 0);
        if (historyNowEmpty)
            bytes += RevisionHistoryOverheadBytes;
        return bytes;
    }

    /// <summary>
    /// Bytes charged when a new MVCC entry is added to entry.MvccEntries.
    /// Pass dictionaryJustCreated=true when entry.MvccEntries was null/empty before this call.
    /// </summary>
    internal static long MvccEntryAddedBytes(bool dictionaryJustCreated, byte[]? snapshotValue)
    {
        long bytes = MvccEntryOverheadBytes + (snapshotValue?.Length ?? 0);
        if (dictionaryJustCreated)
            bytes += DictionaryOverheadBytes;
        return bytes;
    }

    /// <summary>
    /// Bytes to reclaim when a single MVCC entry is removed from entry.MvccEntries.
    /// Pass dictionaryNowEmpty=true when the dictionary is empty after this removal,
    /// so the one-time DictionaryOverheadBytes charged on first add is also reclaimed.
    /// </summary>
    internal static long MvccEntryRemovedBytes(bool dictionaryNowEmpty, byte[]? snapshotValue)
    {
        long bytes = MvccEntryOverheadBytes + (snapshotValue?.Length ?? 0);
        if (dictionaryNowEmpty)
            bytes += DictionaryOverheadBytes;
        return bytes;
    }

    public static long EstimateEntryBytes(string key, KeyValueEntry entry)
    {
        long bytes = (key.Length * sizeof(char)) + (entry.Value?.Length ?? 0) + EntryOverheadBytes;
        bytes += EstimateRevisionBytes(entry.Revisions);
        bytes += EstimateMvccBytes(entry.MvccEntries);
        return bytes;
    }

    private static long EstimateRevisionBytes(KeyValueRevisionHistory? revisions)
    {
        if (revisions is null || revisions.Count == 0)
            return 0;

        long bytes = RevisionHistoryOverheadBytes;

        foreach (KeyValuePair<long, KeyValueRevisionEntry> kv in revisions)
            bytes += sizeof(long) + RevisionEntryOverheadBytes + (kv.Value.Value?.Length ?? 0);

        return bytes;
    }

    private static long EstimateMvccBytes(Dictionary<HLCTimestamp, KeyValueMvccEntry>? mvccEntries)
    {
        if (mvccEntries is null || mvccEntries.Count == 0)
            return 0;

        long bytes = DictionaryOverheadBytes;

        foreach (KeyValueMvccEntry mvccEntry in mvccEntries.Values)
            bytes += MvccEntryOverheadBytes + (mvccEntry.Value?.Length ?? 0);

        return bytes;
    }
}

namespace Kahuna.Server.KeyValues;

using Kommander.Time;

/// <summary>
/// Approximate in-memory footprint of a store entry for budget checks. Figures are intentionally
/// rough (object headers, dictionary buckets, etc. are fudged) but include value bytes plus
/// per-entry revision and MVCC metadata so hot keys with large histories count toward
/// <see cref="Kahuna.Server.Configuration.KahunaConfiguration.MaxBytesPerActor"/>.
/// In-place metadata growth between trim cycles is reconciled when collection trims survivors;
/// value-only mutations use <see cref="KeyValueContext.AdjustEntryValueBytes"/>.
/// </summary>
internal static class KeyValueStoreAccounting
{
    public const int EntryOverheadBytes = 128;
    internal const int DictionaryOverheadBytes = 64;
    internal const int RevisionEntryOverheadBytes = 16;
    private const int MvccEntryOverheadBytes = 64;

    /// <summary>
    /// Bytes charged when a new revision entry is archived into entry.Revisions.
    /// Pass dictionaryJustCreated=true when entry.Revisions was null before this call.
    /// </summary>
    internal static long EstimateRevisionAddedBytes(bool dictionaryJustCreated, byte[]? archivedValue)
    {
        long bytes = sizeof(long) + RevisionEntryOverheadBytes + (archivedValue?.Length ?? 0);
        if (dictionaryJustCreated)
            bytes += DictionaryOverheadBytes;
        return bytes;
    }

    /// <summary>
    /// Bytes to reclaim when a single revision entry is removed from entry.Revisions.
    /// </summary>
    internal static long EstimateRevisionRemovedBytes(byte[]? archivedValue)
        => sizeof(long) + RevisionEntryOverheadBytes + (archivedValue?.Length ?? 0);

    public static long EstimateEntryBytes(string key, KeyValueEntry entry)
    {
        long bytes = (key.Length * sizeof(char)) + (entry.Value?.Length ?? 0) + EntryOverheadBytes;
        bytes += EstimateRevisionBytes(entry.Revisions);
        bytes += EstimateMvccBytes(entry.MvccEntries);
        return bytes;
    }

    private static long EstimateRevisionBytes(Dictionary<long, KeyValueRevisionEntry>? revisions)
    {
        if (revisions is null || revisions.Count == 0)
            return 0;

        long bytes = DictionaryOverheadBytes;

        foreach (KeyValueRevisionEntry revision in revisions.Values)
            bytes += sizeof(long) + RevisionEntryOverheadBytes + (revision.Value?.Length ?? 0);

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

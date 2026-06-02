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
    private const int DictionaryOverheadBytes = 64;
    private const int RevisionEntryOverheadBytes = 16;
    private const int MvccEntryOverheadBytes = 64;

    public static long EstimateEntryBytes(string key, KeyValueEntry entry)
    {
        long bytes = (key.Length * sizeof(char)) + (entry.Value?.Length ?? 0) + EntryOverheadBytes;
        bytes += EstimateRevisionBytes(entry.Revisions);
        bytes += EstimateMvccBytes(entry.MvccEntries);
        return bytes;
    }

    private static long EstimateRevisionBytes(Dictionary<long, byte[]?>? revisions)
    {
        if (revisions is null || revisions.Count == 0)
            return 0;

        long bytes = DictionaryOverheadBytes;

        foreach (byte[]? value in revisions.Values)
            bytes += sizeof(long) + RevisionEntryOverheadBytes + (value?.Length ?? 0);

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

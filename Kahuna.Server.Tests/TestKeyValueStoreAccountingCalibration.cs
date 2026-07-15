
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that KeyValueStoreAccounting constants are reasonably calibrated against
/// hand-computed 64-bit .NET managed object sizes. Estimates must fall within a stated
/// tolerance of the expected structural sizes; value bytes are charged 1:1 (exact).
///
/// Tolerance is ±60 %. The constants deliberately err on the side of over-counting
/// (earlier eviction) rather than under-counting, so the actual floor for over-counting
/// is not bounded below by the tolerance — only the upper bound matters for correctness
/// (we should not be charging 2x the real cost). The tolerance is intentionally wide
/// because the constants cover allocation patterns (initial capacity, struct alignment,
/// dictionary object fields) that vary slightly across .NET minor versions.
/// </summary>
public class TestKeyValueStoreAccountingCalibration
{
    // ---------------------------------------------------------------------------
    // Hand-computed structural sizes for 64-bit .NET 8
    // ---------------------------------------------------------------------------
    //
    // HLCTimestamp (readonly record struct): int N(4) + long L(8) + uint C(4) + padding = 24 bytes
    //   (measured via Unsafe.SizeOf; sequential layout pads the struct to 24)
    //
    // KeyValueMvccEntry (sealed class): measured 112 bytes on heap
    //   object header        16
    //   byte[]? Value ref     8
    //   HLCTimestamp Expires 24
    //   long Revision         8
    //   HLCTimestamp LastUsed 24
    //   HLCTimestamp LastMod  24
    //   KeyValueState (int)   4
    //   bool NoRevision + pad (packed by the runtime's auto class layout)
    //                      = 112 bytes on heap
    //
    // Dictionary<HLCTimestamp,KeyValueMvccEntry> slot in _entries[]:
    //   hashCode(4) + next(4) + key(HLCTimestamp=24) + value-ref(8) = 40 bytes
    //
    // Dictionary object + initial _buckets + _entries array headers (capacity ~4):
    //   dict object fields   ~96
    //   _buckets int[4]      ~32
    //   _entries header      ~16
    //                      = ~144 bytes base (we charge DictionaryOverheadBytes=192, ~33% over)
    //
    // KeyValueRevisionEntry (readonly record struct): measured 64 bytes
    //   byte[]? Value ref     8
    //   HLCTimestamp LastMod 24
    //   HLCTimestamp Expires 24
    //   KeyValueState (int)   4
    //   padding               4
    //                      = 64 bytes (stored inline in revision array slots)
    //
    // KeyValueRevisionHistory (sealed class, initial capacity 1):
    //   object header       16
    //   _items ref           8
    //   _count int           4
    //   padding              4
    //                      = 32 bytes object
    //   initial array[1]:  16 (header) + 1 × 72 (slot) = 88 bytes
    //   total at new()     = 120 bytes; we charge RevisionHistoryOverheadBytes(48) + per-slot(72) = 120
    //
    // (long, KeyValueRevisionEntry) slot in the history array:
    //   long key(8) + struct(64) = 72 bytes per slot

    private const double Tolerance = 0.60; // ±60 %

    private static void AssertWithinTolerance(long estimate, long expected, string label)
    {
        double ratio = (double)estimate / expected;
        Assert.True(
            ratio >= (1.0 - Tolerance) && ratio <= (1.0 + Tolerance),
            $"{label}: estimate={estimate} expected={expected} ratio={ratio:F2} (must be within ±{Tolerance:P0})");
    }

    // ---------------------------------------------------------------------------
    // Value bytes are charged 1:1 — assert exact, independent of structural overhead
    // ---------------------------------------------------------------------------

    [Fact]
    public void ValueBytes_ChargedExactly()
    {
        const string key = "k";
        byte[] value = new byte[1000];

        KeyValueEntry entryNoValue = new() { Value = null };
        KeyValueEntry entryWithValue = new() { Value = value };

        long withoutValue = KeyValueStoreAccounting.EstimateEntryBytes(key, entryNoValue);
        long withValue    = KeyValueStoreAccounting.EstimateEntryBytes(key, entryWithValue);

        Assert.Equal(value.Length, withValue - withoutValue);
    }

    // ---------------------------------------------------------------------------
    // MVCC dictionary overhead: DictionaryOverheadBytes charged once, then per-entry
    // ---------------------------------------------------------------------------

    [Fact]
    public void MvccDictionary_BaseOverhead_WithinTolerance()
    {
        // The DictionaryOverheadBytes constant should reflect the dict object +
        // initial backing arrays. Hand-computed base: ~144 bytes (see header comment).
        const long handComputedBase = 144;
        AssertWithinTolerance(KeyValueStoreAccounting.DictionaryOverheadBytes, handComputedBase,
            "DictionaryOverheadBytes vs hand-computed dict+arrays base");
    }

    [Fact]
    public void MvccEntry_PerEntryOverhead_WithinTolerance()
    {
        const string key = "k";

        KeyValueEntry entryEmpty = new() { Value = null };
        KeyValueEntry entryOneMvcc = new() { Value = null };
        entryOneMvcc.MvccEntries = new();
        entryOneMvcc.MvccEntries[HLCTimestamp.Zero] = new KeyValueMvccEntry { Value = null };

        long baseBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entryEmpty);
        long oneEntry  = KeyValueStoreAccounting.EstimateEntryBytes(key, entryOneMvcc);
        long charged   = oneEntry - baseBytes; // DictionaryOverheadBytes + MvccEntryOverheadBytes

        // Hand-computed: dict base (144) + per-entry object+slot (112+40=152) = 296
        const long handComputed = 296;
        AssertWithinTolerance(charged, handComputed, "MVCC dict-base + one entry overhead");
    }

    [Fact]
    public void MvccEntry_SecondEntry_IncrementalCostWithinTolerance()
    {
        const string key = "k";

        KeyValueEntry entryOne = new() { Value = null };
        entryOne.MvccEntries = new();
        entryOne.MvccEntries[new HLCTimestamp(0, 1, 0)] = new KeyValueMvccEntry { Value = null };

        KeyValueEntry entryTwo = new() { Value = null };
        entryTwo.MvccEntries = new();
        entryTwo.MvccEntries[new HLCTimestamp(0, 1, 0)] = new KeyValueMvccEntry { Value = null };
        entryTwo.MvccEntries[new HLCTimestamp(0, 2, 0)] = new KeyValueMvccEntry { Value = null };

        long oneBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entryOne);
        long twoBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entryTwo);
        long marginal = twoBytes - oneBytes; // MvccEntryOverheadBytes only (no second dict charge)

        // Hand-computed: object(112) + slot(40) = 152
        const long handComputed = 152;
        AssertWithinTolerance(marginal, handComputed, "MVCC second-entry marginal cost");
    }

    // ---------------------------------------------------------------------------
    // Revision dictionary overhead
    // ---------------------------------------------------------------------------

    [Fact]
    public void RevisionEntry_PerEntryOverhead_WithinTolerance()
    {
        const string key = "k";

        KeyValueEntry entryEmpty = new() { Value = null };
        KeyValueEntry entryOneRev = new() { Value = null };
        entryOneRev.Revisions = new();
        entryOneRev.Revisions[1L] = new KeyValueRevisionEntry(null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);

        long baseBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entryEmpty);
        long oneEntry  = KeyValueStoreAccounting.EstimateEntryBytes(key, entryOneRev);
        long charged   = oneEntry - baseBytes;

        // Hand-computed: RevisionHistoryOverheadBytes(48) + sizeof(long=8) + struct(64) = 120
        const long handComputed = 120;
        AssertWithinTolerance(charged, handComputed, "Revision history base + one entry overhead");
    }

    [Fact]
    public void RevisionEntry_SecondEntry_IncrementalCostWithinTolerance()
    {
        const string key = "k";

        KeyValueEntry entryOne = new() { Value = null };
        entryOne.Revisions = new();
        entryOne.Revisions[1L] = new KeyValueRevisionEntry(null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);

        KeyValueEntry entryTwo = new() { Value = null };
        entryTwo.Revisions = new();
        entryTwo.Revisions[1L] = new KeyValueRevisionEntry(null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);
        entryTwo.Revisions[2L] = new KeyValueRevisionEntry(null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);

        long oneBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entryOne);
        long twoBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entryTwo);
        long marginal = twoBytes - oneBytes;

        // Hand-computed: array grows from cap 1 to cap 2, adding exactly one new 72-byte slot.
        // sizeof(long=8) + struct(64) = 72
        const long handComputed = 72;
        AssertWithinTolerance(marginal, handComputed, "Revision second-entry marginal cost");
    }

    // ---------------------------------------------------------------------------
    // IsOverStoreBudget consumes the same EstimateEntryBytes signal — verified by
    // the existing cluster tests; here we just confirm the accounting path exists.
    // ---------------------------------------------------------------------------

    [Fact]
    public void EstimateEntryBytes_IncludesBothMvccAndRevisions()
    {
        const string key = "k";

        KeyValueEntry entryBoth = new() { Value = null };
        entryBoth.MvccEntries = new();
        entryBoth.MvccEntries[HLCTimestamp.Zero] = new KeyValueMvccEntry { Value = null };
        entryBoth.Revisions = new();
        entryBoth.Revisions[1L] = new KeyValueRevisionEntry(null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);

        KeyValueEntry entryNeither = new() { Value = null };

        long diff = KeyValueStoreAccounting.EstimateEntryBytes(key, entryBoth)
                  - KeyValueStoreAccounting.EstimateEntryBytes(key, entryNeither);

        // Both dicts add their overheads — must be materially larger than zero.
        Assert.True(diff > 100, $"Expected MVCC+revision overhead >100 bytes, got {diff}");
    }
}

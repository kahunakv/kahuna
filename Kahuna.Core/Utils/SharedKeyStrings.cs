using System.IO.Hashing;
using System.Runtime.InteropServices;

namespace Kahuna.Utils;

/// <summary>
/// Gives one string instance to the equal key strings that a node keeps in several stores.
///
/// <para>A committed transaction leaves its keys in the transaction record, in one completion receipt for
/// each key and in one settled identity for each key. Each store reads the key from a log entry of its own,
/// so each decodes a string of its own, and one key of 30 characters is then four or more strings of 88 bytes
/// for the 90 seconds that the stores keep the transaction. A store passes each key through
/// <see cref="Share(string, out bool)"/> before it keeps it. The stores of one node use one instance of this
/// class, so they keep the same string.</para>
///
/// <para>The cache is a table of fixed size and it can forget a string: a string that leaves the table is
/// shared no more, and a later equal string becomes the shared one. A caller gets a string that is equal to
/// the one it passed, so the cache changes no result, only how many copies of a string the heap holds. The
/// table has two slots for each hash value. A lookup that finds its string in the second slot moves it to
/// the first, and a new string takes the first slot and moves the other to the second, so two keys in
/// constant use with the same hash value do not replace each other.</para>
///
/// <para>Thread-safe with no lock. Two threads that write to one pair of slots at the same time can lose one
/// of the two strings or leave one string in both slots. The next lookup of the lost string stores it
/// again.</para>
/// </summary>
internal sealed class SharedKeyStrings
{
    /// <summary>Pairs of slots of the default table: 65,536 slots, one array of 512 KB.</summary>
    internal const int DefaultSlotPairs = 1 << 15;

    private static readonly long Seed = Random.Shared.NextInt64();

    private readonly int pairMask;

    // Created on the first use: a store that holds no transaction state holds no table.
    private string?[]? slots;

    /// <param name="slotPairs">Number of slot pairs; a power of two.</param>
    public SharedKeyStrings(int slotPairs = DefaultSlotPairs)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(slotPairs, 1);

        if (!int.IsPow2(slotPairs))
            throw new ArgumentOutOfRangeException(nameof(slotPairs), slotPairs, "The number of slot pairs must be a power of two.");

        pairMask = slotPairs - 1;
    }

    /// <summary>Returns the shared instance of <paramref name="value"/>.</summary>
    public string Share(string value) => Share(value, out _);

    /// <summary>
    /// Returns the shared instance of <paramref name="value"/>: a string the cache already holds that is equal
    /// to it, or <paramref name="value"/> itself, which the cache then holds.
    /// </summary>
    /// <param name="added">True when the cache held no equal string, so the returned instance is new to the
    /// stores of the node. The store that receives true accounts for the bytes of the string.</param>
    public string Share(string value, out bool added)
    {
        added = false;

        if (value.Length == 0)
            return string.Empty;

        string?[] table = slots ?? CreateTable();

        int first = ((int)XxHash3.HashToUInt64(MemoryMarshal.AsBytes(value.AsSpan()), Seed) & pairMask) << 1;

        ref string? recentSlot = ref table[first];
        ref string? olderSlot = ref table[first + 1];

        string? recent = Volatile.Read(ref recentSlot);
        if (recent is not null && (ReferenceEquals(recent, value) || string.Equals(recent, value)))
            return recent;

        string? older = Volatile.Read(ref olderSlot);
        if (older is not null && (ReferenceEquals(older, value) || string.Equals(older, value)))
        {
            Volatile.Write(ref olderSlot, recent);
            Volatile.Write(ref recentSlot, older);
            return older;
        }

        Volatile.Write(ref olderSlot, recent);
        Volatile.Write(ref recentSlot, value);
        added = true;
        return value;
    }

    private string?[] CreateTable()
    {
        string?[] created = new string?[(pairMask + 1) << 1];
        return Interlocked.CompareExchange(ref slots, created, null) ?? created;
    }
}

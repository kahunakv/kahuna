using Kommander.Time;

namespace Kahuna.Utils;

/// <summary>
/// The hash of a key that is a string together with the identity of a transaction: a completion receipt, or a
/// settled identity of the ledger. A key of this kind stores the hash it computes here, and a sharded map
/// reads the high bits to select a shard, so every input must reach every bit.
/// </summary>
internal static class TransactionKeyHash
{
    /// <summary>
    /// Folds <paramref name="transactionId"/> and <paramref name="epoch"/> into the hash of
    /// <paramref name="key"/>. The string hash is the randomized one of the runtime, which keeps its resistance
    /// to chosen keys; the numbers are folded in with multiplications and a final mix, which costs a small
    /// part of the string hash. The combine methods of <see cref="HashCode"/> give the same quality for
    /// several times the cost of this fold.
    /// </summary>
    public static int Combine(string key, HLCTimestamp transactionId, long epoch = 0)
    {
        uint hash = (uint)key.GetHashCode();

        hash = (hash ^ (uint)transactionId.L) * 0x9E3779B1u;
        hash = (hash ^ (uint)(transactionId.L >> 32) ^ transactionId.C) * 0x85EBCA6Bu;
        hash = (hash ^ (uint)transactionId.N ^ (uint)epoch ^ (uint)(epoch >> 32)) * 0xC2B2AE35u;

        // The final mix of MurmurHash3: the multiplications above move bits upward only.
        hash ^= hash >> 16;
        hash *= 0x85EBCA6Bu;
        hash ^= hash >> 13;
        hash *= 0xC2B2AE35u;
        hash ^= hash >> 16;

        return (int)hash;
    }
}

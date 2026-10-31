using System.Text;
using Kahuna.Server.Configuration;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The key and value sizes a client entry point accepts on a write or a lock acquire, and the item count it accepts
/// in one batch call. Without them the only bound is the transport's request size: one accepted write is carried by
/// the store, the Raft entry, the WAL and the backend of every replica — or, past the Raft message limit, never
/// replicates at all — and one batch of tiny items costs a routing decision and an actor message per item.
///
/// <para>Applied only where a client's request enters (REST handlers, unary gRPC calls and the client batch stream).
/// The inter-node path forwards requests a client entry point already admitted, and an embedded host writes through
/// <see cref="IKahuna"/> with no bound. Zero or less means no bound.</para>
/// </summary>
public readonly record struct KeyValueInputLimits(int MaxKeyBytes, int MaxValueBytes, int MaxBatchItems = 0, int MaxTransactionMutations = 0)
{
    public static KeyValueInputLimits From(KahunaConfiguration configuration) =>
        new(configuration.MaxKeyBytes, configuration.MaxValueBytes, configuration.MaxBatchItems, configuration.MaxTransactionMutations);

    /// <summary>Whether a batch of <paramref name="itemCount"/> items fits in <see cref="MaxBatchItems"/>.</summary>
    public bool BatchFits(int itemCount) => MaxBatchItems <= 0 || itemCount <= MaxBatchItems;

    /// <summary>
    /// Whether <paramref name="key"/> fits in <see cref="MaxKeyBytes"/> UTF-8 bytes. A null key fits: the entry point
    /// refuses a missing key by its own check. One UTF-16 char takes 1 to 3 UTF-8 bytes, so most keys are decided by
    /// their length without a count.
    /// </summary>
    public bool KeyFits(string? key)
    {
        if (MaxKeyBytes <= 0 || key is null || key.Length * 3L <= MaxKeyBytes)
            return true;

        if (key.Length > MaxKeyBytes)
            return false;

        return Encoding.UTF8.GetByteCount(key) <= MaxKeyBytes;
    }

    /// <summary>Whether a value of <paramref name="byteCount"/> bytes fits in <see cref="MaxValueBytes"/>.</summary>
    public bool ValueFits(int byteCount) => MaxValueBytes <= 0 || byteCount <= MaxValueBytes;

    /// <summary>Whether a value fits; a null value (one the caller left absent) always does.</summary>
    public bool ValueFits(byte[]? value) => value is null || ValueFits(value.Length);
}

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Whether a durable prepared intent is still awaiting its transaction's canonical decision, or has been
/// resolved to commit (its value materializes) or abort (its value is discarded). Resolution is recorded on the
/// intent so commit/abort apply is idempotent under replay; the intent is removed only after it is resolved.
/// </summary>
internal enum PreparedIntentResolution
{
    Pending = 0,
    Committed = 1,
    Aborted = 2
}

/// <summary>
/// A durable, partition-scoped prepared mutation of one key belonging to one transaction attempt, keyed by
/// <c>(TransactionId, Epoch, Key)</c>. After its prepare record commits this store — not the actor-local
/// <c>KeyValueEntry.WriteIntent</c> — is the authority for the pending mutation, so a follower, a cold restart, a
/// cache miss, or a new leader reconstructs the same unresolved intent. The mutation is invisible as a value
/// until the transaction's canonical decision says commit.
/// </summary>
internal sealed record PreparedIntent(
    HLCTimestamp TransactionId,
    long Epoch,
    string Key,
    long ManifestHash,
    string RecordAnchorKey,
    HLCTimestamp CommitTimestamp,
    // the complete proposed mutation
    KeyValueState State,
    byte[]? Value,
    string? Bucket,
    long Revision,
    HLCTimestamp Expires,
    bool NoRevision,
    // the committed base this prepare validated against
    long BaseRevision,
    KeyValueState BaseState,
    HLCTimestamp RecoveryDeadline,
    PreparedIntentResolution Resolution)
{
    public bool IsPending => Resolution == PreparedIntentResolution.Pending;

    public bool IsResolved => Resolution != PreparedIntentResolution.Pending;
}

/// <summary>
/// A stable digest of a prepared intent's proposed mutation and validated base, used to distinguish an exact
/// duplicate prepare (idempotent) from a corrupt re-prepare of the same <c>(TransactionId, Epoch, Key)</c> with a
/// different mutation (rejected). It deliberately excludes the resolution status and recovery deadline, which are
/// not part of the mutation's identity. Fixed FNV-1a; never <see cref="object.GetHashCode"/>.
/// </summary>
internal static class PreparedIntentDigest
{
    public static long Compute(PreparedIntent intent)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong h = offset;
        h = Fold(h, (long)intent.State, prime);
        h = FoldBytes(h, intent.Value, prime);
        h = FoldString(h, intent.Bucket ?? "\0<null>", prime);
        h = Fold(h, intent.Revision, prime);
        h = FoldString(h, intent.Expires.ToString(), prime);
        h = Fold(h, intent.NoRevision ? 1 : 0, prime);
        h = Fold(h, intent.BaseRevision, prime);
        h = Fold(h, (long)intent.BaseState, prime);
        h = FoldString(h, intent.CommitTimestamp.ToString(), prime);

        return unchecked((long)h);
    }

    public static bool Matches(PreparedIntent a, PreparedIntent b) => Compute(a) == Compute(b);

    private static ulong FoldBytes(ulong h, byte[]? value, ulong prime)
    {
        if (value is null)
        {
            h ^= 0xFF;
            h *= prime;
            return h;
        }

        foreach (byte b in value)
        {
            h ^= b;
            h *= prime;
        }

        h ^= (ulong)value.Length;
        h *= prime;
        return h;
    }

    private static ulong FoldString(ulong h, string value, ulong prime)
    {
        foreach (char c in value)
        {
            h ^= c;
            h *= prime;
        }

        h ^= (ulong)value.Length;
        h *= prime;
        return h;
    }

    private static ulong Fold(ulong h, long value, ulong prime)
    {
        ulong v = unchecked((ulong)value);
        for (int i = 0; i < 8; i++)
        {
            h ^= v & 0xFF;
            h *= prime;
            v >>= 8;
        }

        return h;
    }
}

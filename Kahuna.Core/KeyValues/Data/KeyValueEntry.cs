
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a key/value entry that holds metadata, historical versions and state.
/// Used to manage the lifecycle and multi-version concurrency control (MVCC)
/// of key-value pairs, providing support for persistence, revisions, expiration and lock management.
/// </summary>
internal sealed class KeyValueEntry
{
    /// <summary>
    /// The current bucket of the key if any
    /// </summary>
    public string? Bucket { get; set; }
    
    /// <summary>
    /// The current value of the key.
    /// </summary>
    public byte[]? Value { get; set; }
    
    /// <summary>
    /// HLC timestamp when the key/value will expire
    /// </summary>
    public HLCTimestamp Expires { get; set; }
    
    /// <summary>
    /// Current modification revision
    /// </summary>
    public long Revision { get; set; }

    /// <summary>
    /// The highest revision confirmed written to disk. Entry is dirty (ineligible for eviction
    /// until the flush safety window elapses) when <see cref="Revision"/> &gt; FlushedRevision.
    /// Ephemeral entries set this equal to <see cref="Revision"/> at commit time — they have no
    /// disk copy, so the guard never applies. Entries loaded from disk are initialized to
    /// <see cref="Revision"/> as well. Default -1 ensures a newly-committed persistent entry is
    /// immediately marked dirty before any flush can occur.
    /// </summary>
    public long FlushedRevision { get; set; } = -1;
    
    /// <summary>
    /// HLC timestamp of the last time the key/value was used
    /// </summary>
    public HLCTimestamp LastUsed { get; set; }
    
    /// <summary>
    /// HLC timestamp of the last time the key was modified
    /// </summary>
    public HLCTimestamp LastModified { get; set; }

    /// <summary>
    /// Represents a potential write intent to modify the key
    /// </summary>
    public KeyValueWriteIntent? WriteIntent { get; set; }
    
    /// <summary>
    /// Represents an active replication intent for the key
    /// </summary>
    public KeyValueReplicationIntent? ReplicationIntent { get; set; }
    
    /// <summary>
    /// Recently archived revisions, keyed by revision number. Each entry carries the value plus
    /// the HLC timestamp at which it was committed, enabling snapshot-isolated reads via
    /// <see cref="TryGetRevisionAtOrBefore"/>. Bounded by RevisionsToKeepCached / RevisionRetention.
    /// </summary>
    public Dictionary<long, KeyValueRevisionEntry>? Revisions { get; set; }

    /// <summary>
    /// Multiversion Concurrency Control (MVCC) values per TransactionId
    /// </summary>
    public Dictionary<HLCTimestamp, KeyValueMvccEntry>? MvccEntries { get; set; }

    /// <summary>
    /// Current state of the key
    /// </summary>
    public KeyValueState State { get; set; } = KeyValueState.Set;

    /// <summary>
    /// Running byte estimate for this entry (excluding the key-length contribution).
    /// Initialized by <see cref="KeyValueContext.InsertStoreEntry"/> on first insertion and
    /// maintained incrementally by AdjustEstimatedEntryBytes / AdjustEntryValueBytes so that
    /// accounting lookups on the write path are O(1) instead of O(history).
    /// </summary>
    internal long CachedBytes;

    /// <summary>
    /// Returns true when this entry's latest committed revision may not yet be on disk and the
    /// entry is therefore ineligible for eviction.  Both conditions must hold: the revision counter
    /// is ahead of what was last confirmed flushed, AND the entry was modified recently enough that
    /// the flush cycle may not have run yet (time-guard proxy for the missing flush-ack).
    /// Ephemeral entries always have FlushedRevision == Revision and return false.
    /// </summary>
    public bool IsDirty(long safetyWindowMs, HLCTimestamp currentTime) =>
        Revision > FlushedRevision &&
        (currentTime - LastModified) < TimeSpan.FromMilliseconds(safetyWindowMs);

    /// <summary>
    /// Finds the most recent archived revision whose <see cref="KeyValueRevisionEntry.LastModified"/>
    /// is at or before <paramref name="snapshot"/>. Used to serve a snapshot-isolated read when the
    /// current revision (<see cref="LastModified"/>) is newer than the reader's snapshot.
    ///
    /// Only the archived history is consulted (the caller checks the live revision first). Returns
    /// false when no revision ≤ the snapshot is retained — either the key did not exist at the
    /// snapshot, or the relevant revision was already trimmed (best effort, bounded by retention).
    /// </summary>
    public bool TryGetRevisionAtOrBefore(HLCTimestamp snapshot, out long revisionNumber, out KeyValueRevisionEntry revision)
    {
        revisionNumber = -1;
        revision = default;

        if (Revisions is null)
            return false;

        bool found = false;

        foreach ((long candidateNumber, KeyValueRevisionEntry candidate) in Revisions)
        {
            if (candidate.LastModified > snapshot)
                continue;

            // Revision numbers are monotonic with commit order, so the highest revision at or
            // before the snapshot is the visible one.
            if (!found || candidateNumber > revisionNumber)
            {
                revisionNumber = candidateNumber;
                revision = candidate;
                found = true;
            }
        }

        return found;
    }
}
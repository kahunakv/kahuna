
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
    /// The highest revision confirmed written to disk. Entry is dirty (ineligible for eviction)
    /// while <see cref="Revision"/> &gt; FlushedRevision — advanced only by a flush acknowledgement
    /// from the background writer after a confirmed store, never by elapsed time.
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
    /// Stored as a compact sorted array (<see cref="KeyValueRevisionHistory"/>) rather than a
    /// dictionary, eliminating per-bucket and per-entry hash-table overhead for the small n ≤ 17 bound.
    /// </summary>
    public KeyValueRevisionHistory? Revisions { get; set; }

    /// <summary>
    /// Transaction whose committed mutation most recently advanced this resident head through the
    /// archival apply path. This actor-local proof distinguishes a completed apply from a cache entry
    /// that merely happens to have matching revision metadata. It is intentionally transient; a
    /// disk-loaded entry instead uses its flushed revision plus durable receipt as apply proof.
    /// </summary>
    public HLCTimestamp LastAppliedTransactionId { get; set; }

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
    /// The store key this entry is filed under. Set by InsertStoreEntry; cleared on removal.
    /// Required so the LRU head-pop path can evict by key without a reverse store lookup.
    /// </summary>
    internal string? StoreKey;

    /// <summary>
    /// Intrusive doubly-linked LRU list — previous node (toward head / coldest).
    /// Null when this entry is the coldest in the actor or is not linked.
    /// </summary>
    internal KeyValueEntry? LruPrev;

    /// <summary>
    /// Intrusive doubly-linked LRU list — next node (toward tail / hottest).
    /// Null when this entry is the hottest in the actor or is not linked.
    /// </summary>
    internal KeyValueEntry? LruNext;

    /// <summary>
    /// Returns true when this entry's latest committed revision is not yet confirmed on disk and the
    /// entry is therefore ineligible for eviction. The single condition is that the revision counter
    /// is ahead of what the background writer has acknowledged flushing; a dirty entry stays pinned
    /// until that acknowledgement arrives, regardless of how much time has passed — evicting it early
    /// would drop the only cached copy of a committed-but-unflushed revision.
    /// Ephemeral entries always have FlushedRevision == Revision and return false.
    /// </summary>
    public bool IsDirty() => Revision > FlushedRevision;

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

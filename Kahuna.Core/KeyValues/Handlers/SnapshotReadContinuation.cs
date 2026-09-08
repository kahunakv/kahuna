
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stage-3 reconciliation for a snapshot (as-of-timestamp) point read that needs the persisted
/// history: either the key is not resident, or it is resident but its in-memory revision archive
/// cannot answer for the reader's timestamp. Shared by TryGet (responseType=Get) and TryExists
/// (responseType=Exists).
///
/// <para>Why this exists. Before it, a snapshot read on a cache miss loaded the head with the
/// actor mailbox parked, and an archive miss called the backend's revision-history read
/// <b>synchronously on the actor thread</b>. That read walks every revision the key ever had, so
/// under sustained writes one cold key stalled every key on its actor (CamusDB feature 80af367a:
/// leader reads 1 ms to 40-50 ms with the CPU idle). Both reads now run on the backend read
/// scheduler like every other persistent miss, and the head comes back with its newest archived
/// revisions so the next snapshot read against it is an in-memory hit.</para>
///
/// <para>Stage 2 (<see cref="Load"/>) is pure backend work and touches no actor state. Stage 3
/// (<see cref="Execute"/>) reconciles against the resident store — a write may have landed while
/// the read was in flight — installs the hydrated head and its archive with byte accounting, and
/// applies the same snapshot rules the inline path uses.</para>
///
/// <para>Coalescing key: <c>(key, readTimestamp, isExists)</c> in
/// <see cref="KeyValueContext.PendingSnapshotReads"/>. Readers of different snapshots must not share a
/// result, so the timestamp is part of the slot.</para>
/// </summary>
internal sealed class SnapshotReadContinuation : ReadContinuation
{
    private readonly string key;
    private readonly HLCTimestamp readTimestamp;
    private readonly KeyValueResponseType responseType;

    /// <summary>
    /// True when stage 2 must load the head (cache miss); false when the head is resident and only
    /// the persisted history below <see cref="asOfCeiling"/> is needed.
    /// </summary>
    private readonly bool hydrate;

    /// <summary>Highest revision the persisted history may answer with when <see cref="hydrate"/> is false.</summary>
    private readonly long asOfCeiling;

    /// <summary>Stage-2 result when hydrating: the head and its newest archived revisions.</summary>
    internal KeyValueHydration? Hydration { get; private set; }

    /// <summary>Stage-2 as-of answer from the persisted history; meaningful only when <see cref="AsOfConsulted"/>.</summary>
    internal KeyValueEntry? AsOf { get; private set; }

    /// <summary>True when stage 2 consulted the persisted revision history (so a null <see cref="AsOf"/> means "no such revision").</summary>
    internal bool AsOfConsulted { get; private set; }

    internal SnapshotReadContinuation(
        string key,
        HLCTimestamp readTimestamp,
        KeyValueResponseType responseType,
        bool hydrate,
        long asOfCeiling,
        KeyValueReplyRef promise) : base(promise)
    {
        this.key = key;
        this.readTimestamp = readTimestamp;
        this.responseType = responseType;
        this.hydrate = hydrate;
        this.asOfCeiling = asOfCeiling;
    }

    internal override void RemovePendingKey(KeyValueContext context) =>
        context.PendingSnapshotReads.Remove((key, readTimestamp, responseType == KeyValueResponseType.Exists));

    /// <summary>
    /// Stage 2, scheduler thread. Loads what the snapshot needs and nothing more: the head plus its
    /// newest <paramref name="recentRevisions"/> archived revisions when hydrating, and the persisted
    /// as-of row only when neither the head nor those revisions answer for the timestamp. Mutates no
    /// actor-owned state. A continuation expired by the deadline sweep skips the work.
    /// </summary>
    internal void Load(IPersistenceBackend backend, int recentRevisions)
    {
        if (Cancelled)
            return;

        if (!hydrate)
        {
            AsOf = backend.GetKeyValueRevisionAtOrBefore(key, asOfCeiling, readTimestamp);
            AsOfConsulted = true;
            return;
        }

        KeyValueHydration hydration = backend.GetKeyValueWithRecentRevisions(key, recentRevisions);
        Hydration = hydration;

        KeyValueEntry? head = hydration.Head;
        if (head is null || head.LastModified.CompareTo(readTimestamp) <= 0)
            return; // the head is the snapshot's answer

        // Newest first: the first archived revision at-or-before the timestamp is the answer and will
        // be served from the archive at stage 3.
        foreach (KeyValueEntry row in hydration.RecentRevisions)
            if (row.LastModified.CompareTo(readTimestamp) <= 0)
                return;

        // Every revision number in [head - requested, head - 1] has been examined; the answer, if any,
        // is older than the archive window.
        long requested = Math.Min(recentRevisions, head.Revision);
        long ceiling = head.Revision - 1 - requested;
        AsOf = ceiling >= 0 ? backend.GetKeyValueRevisionAtOrBefore(key, ceiling, readTimestamp) : null;
        AsOfConsulted = true;
    }

    internal override void Execute(KeyValueContext context) => Resolve(Reconcile(context));

    /// <summary>
    /// Stage 3 as a pure function of actor state: removes the in-flight registration, reconciles the
    /// stage-2 result against the resident store, installs the hydrated head and archive, and returns
    /// the response every waiter receives. Also used directly by the inline fallback a handler takes
    /// when it runs without an actor context (the handler-level test harness), where the same
    /// stage-2 work is awaited on the scheduler and the result is returned instead of resolved.
    /// </summary>
    internal KeyValueResponse Reconcile(KeyValueContext context)
    {
        // Remove before resolving so any new miss after this resume starts a fresh read.
        RemovePendingKey(context);

        if (Faulted)
            return KeyValueStaticResponses.MustRetryResponse;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock
            .TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (!hydrate)
            return FromPersistedHistory(currentTime);

        KeyValueHydration hydration = Hydration!;
        KeyValueEntry? entry;

        // A write (or another read) may have installed a resident entry while this read was in
        // flight. Prefer the higher revision; never overwrite a resident entry with an older head.
        if (context.Store.TryGetValue(key, out KeyValueEntry? resident)
            && (hydration.Head is null || resident.Revision >= hydration.Head.Revision))
        {
            entry = resident;
        }
        else
        {
            // A disk row (or absence) below this node's committed-head memory is provably stale —
            // the missing committed writes would make the as-of answer wrong. Refuse and let the
            // scheduled convergence repair land before the caller's retry.
            if (BaseHandler.HydratedRowProvablyStale(context, key, hydration.Head))
            {
                return KeyValueStaticResponses.MustRetryResponse;
            }

            if (hydration.Head is null)
            {
                return KeyValueStaticResponses.DoesNotExistContextResponse;
            }

            entry = hydration.Head;
            entry.FlushedRevision = entry.Revision;
            entry.LastUsed = currentTime;
            context.InsertStoreEntry(key, entry);
            ArchiveRecentRevisions(context, entry, hydration.RecentRevisions);
        }

        // ── The inline snapshot rules, against the (now resident) entry ─────────────────────
        if (entry.LastModified.CompareTo(readTimestamp) <= 0)
        {
            context.TouchEntry(entry, currentTime);
            return ToResponse(entry.Value, entry.Revision, entry.Expires, entry.LastUsed, entry.LastModified, entry.State, currentTime);
        }

        if (entry.TryGetRevisionAtOrBefore(readTimestamp, out long snapRevision, out KeyValueRevisionEntry snapshot))
        {
            return ToResponse(snapshot.Value, snapRevision, snapshot.Expires, currentTime, snapshot.LastModified, snapshot.State, currentTime);
        }

        // A head jump skipped revisions whose flush requests may still be queued: the persisted
        // history cannot answer for the skipped window yet. Fail closed; the caller retries.
        if (entry.SnapshotAtRiskFromUnflushedGap(readTimestamp))
        {
            return KeyValueStaticResponses.MustRetryResponse;
        }

        if (AsOfConsulted)
            return FromPersistedHistory(currentTime);

        // The head loaded at stage 2 answered for the snapshot then, but a resident entry moved past
        // it and its archive cannot answer now. Retrying finds the entry resident and dispatches the
        // persisted-history read for exactly the revisions that are missing.
        return KeyValueStaticResponses.MustRetryResponse;
    }

    private KeyValueResponse FromPersistedHistory(HLCTimestamp currentTime)
    {
        KeyValueEntry? asOf = AsOf;
        if (asOf is null)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        return ToResponse(asOf.Value, asOf.Revision, asOf.Expires, currentTime, asOf.LastModified, asOf.State, currentTime);
    }

    /// <summary>
    /// Installs the hydrated head's newest revisions into its in-memory archive, charging the store
    /// budget the way <c>BaseHandler.ApplyCommittedHead</c> does. Must run after
    /// <see cref="KeyValueContext.InsertStoreEntry"/> so the entry's byte estimate is initialised.
    /// </summary>
    private static void ArchiveRecentRevisions(KeyValueContext context, KeyValueEntry entry, List<KeyValueEntry> recent)
    {
        if (recent.Count == 0)
            return;

        bool historyJustCreated = entry.Revisions is null || entry.Revisions.Count == 0;
        entry.Revisions ??= new();

        // The archive is a sorted array that is appended in ascending revision order (the write path
        // archives one revision at a time, always the newest). The hydration list is newest first, so
        // walk it from the end.
        for (int i = recent.Count - 1; i >= 0; i--)
        {
            KeyValueEntry row = recent[i];
            if (row.Revision < 0 || row.Revision >= entry.Revision || entry.Revisions.ContainsKey(row.Revision))
                continue;

            entry.Revisions[row.Revision] = new KeyValueRevisionEntry(row.Value, row.LastModified, row.Expires, row.State);
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(historyJustCreated, row.Value));
            historyJustCreated = false;
        }
    }

    private KeyValueResponse ToResponse(
        byte[]? value, long revision, HLCTimestamp expires, HLCTimestamp lastUsed,
        HLCTimestamp lastModified, KeyValueState state, HLCTimestamp currentTime)
    {
        if (state is KeyValueState.Undefined or KeyValueState.Deleted
            || (expires != HLCTimestamp.Zero && expires - currentTime < TimeSpan.Zero))
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        return new(responseType, new ReadOnlyKeyValueEntry(
            responseType == KeyValueResponseType.Get ? value : null,
            revision, expires, lastUsed, lastModified, state));
    }
}

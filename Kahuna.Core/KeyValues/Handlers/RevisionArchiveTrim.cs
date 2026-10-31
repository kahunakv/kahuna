
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Removes archived revisions below a cutoff from an entry's in-memory revision archive while honouring the
/// snapshot-floor boundary. Shared by the archive-time trim in <see cref="BaseHandler.RemoveExpiredRevisions"/>
/// and the flush-time trim the actor runs when flushed revisions are not kept resident
/// (<see cref="Configuration.KahunaConfiguration.TrimFlushedRevisions"/>). The caller decides the cutoff; this
/// routine owns the boundary pinning, the coverage bookkeeping and the byte accounting so both call sites keep
/// the same invariants.
/// </summary>
internal static class RevisionArchiveTrim
{
    /// <summary>
    /// Trims every archived revision strictly below <paramref name="cutoff"/> except the floor-boundary
    /// revision, when a snapshot hold pins one.
    ///
    /// <para>When any snapshot hold is registered (live or lapsed-but-unpurged: protection ends only when the
    /// hold's replicated removal commits), the single highest revision whose
    /// <see cref="KeyValueRevisionEntry.LastModified"/> is at-or-before the reclamation floor is pinned as the
    /// floor-boundary revision and exempted from removal, so the as-of version snapshot readers need survives in
    /// memory beyond the cutoff. The revisions between the boundary and the cutoff are left to disk and reached
    /// by the disk-fallback read paths.</para>
    /// </summary>
    /// <param name="revisionsToRemove">Caller-owned scratch set, cleared on return.</param>
    public static void Trim(KeyValueContext context, KeyValueEntry entry, long cutoff, HashSet<long> revisionsToRemove)
    {
        if (entry.Revisions is null || entry.Revisions.Count == 0)
            return;

        // Determine the floor-boundary revision: the highest revision that would normally
        // be trimmed (< cutoff) but whose timestamp is at-or-before the reclamation floor.
        // We protect exactly this one revision so snapshot reads at floor-timestamp still
        // hit memory without disk I/O for the boundary version itself. The reclamation floor
        // honors every registered hold, expired or not — a hold's protection ends only when
        // its replicated removal (release or purge) commits, never at bare lease expiry — so
        // a lapsed hold that is later revived by a renew still finds its boundary pinned.
        long floorBoundaryRevision = -1;
        SnapshotFloorStore? floorStore = context.SnapshotFloorStore;
        if (floorStore is not null && floorStore.Holds.Count > 0)
        {
            HLCTimestamp protectiveFloor = floorStore.GetProtectiveFloor();
            if (protectiveFloor != HLCTimestamp.Zero)
            {
                foreach (KeyValuePair<long, KeyValueRevisionEntry> kv in entry.Revisions)
                {
                    if (kv.Key < cutoff && kv.Value.LastModified.CompareTo(protectiveFloor) <= 0)
                    {
                        if (kv.Key > floorBoundaryRevision)
                            floorBoundaryRevision = kv.Key;
                    }
                }
            }
        }

        // Record which revision is pinned as the boundary so snapshot reads can tell an
        // authoritative in-memory hit from a boundary hit that sits below trimmed, disk-only
        // revisions. A newly pinned boundary starts with full coverage (Zero — nothing trimmed
        // above it yet); the coverage bound then shrinks as removals above it accumulate below.
        // A stale record from a released floor is left in place: its revision is trimmed by this
        // same pass (it is no longer exempt), after which no lookup can hit it.
        if (floorBoundaryRevision >= 0 && floorBoundaryRevision != entry.FloorBoundaryRevision)
        {
            entry.FloorBoundaryRevision = floorBoundaryRevision;
            entry.FloorBoundaryCoverageEnd = HLCTimestamp.Zero;
        }

        foreach (KeyValuePair<long, KeyValueRevisionEntry> kv in entry.Revisions)
        {
            if (kv.Key < cutoff && kv.Key != floorBoundaryRevision)
                revisionsToRemove.Add(kv.Key);
        }

        // Fail-loud floor guard. The floor pins exactly one in-memory revision: the boundary
        // (newest revision whose LastModified is at-or-before the floor). The loop above exempts
        // it, so a correct trim never schedules it for removal. If it ever appears in the removal
        // set the boundary computation has regressed and a floor-protected version would be dropped
        // from memory — count it (this counter must stay 0 in normal operation) and retain the revision.
        if (BaseHandler.RemovalSetDropsFloorBoundary(revisionsToRemove, floorBoundaryRevision))
        {
            SnapshotFloorMetrics.MissingProtectedVersion.Add(1);
            context.Logger.LogError(
                "Floor-boundary revision {Revision} was scheduled for trimming while a snapshot hold protects it; retaining it instead",
                floorBoundaryRevision);
            revisionsToRemove.Remove(floorBoundaryRevision);
        }

        if (revisionsToRemove.Count == 0)
            return;

        long bytesFreed = 0;

        foreach (long revision in revisionsToRemove)
        {
            entry.Revisions.TryGetValue(revision, out KeyValueRevisionEntry removed);
            entry.Revisions.Remove(revision);
            bytesFreed += KeyValueStoreAccounting.EstimateRevisionRemovedBytes(entry.Revisions.Count == 0, removed.Value);

            // Removing a revision above the pinned boundary opens (or widens) the gap between
            // the boundary and the retention window. Track the earliest timestamp ever trimmed
            // above the boundary: at-or-after it, a boundary hit is no longer authoritative
            // and the read must consult the persisted revision history instead.
            if (entry.FloorBoundaryRevision >= 0 && revision > entry.FloorBoundaryRevision
                && (entry.FloorBoundaryCoverageEnd == HLCTimestamp.Zero
                    || removed.LastModified.CompareTo(entry.FloorBoundaryCoverageEnd) < 0))
                entry.FloorBoundaryCoverageEnd = removed.LastModified;
        }

        context.AdjustEstimatedEntryBytes(entry, -bytesFreed);
        revisionsToRemove.Clear();
    }
}

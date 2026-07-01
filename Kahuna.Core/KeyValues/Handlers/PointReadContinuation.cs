
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stage-3 reconciliation for a non-transactional persistent point-read (latest, non-by-revision)
/// cache miss. Shared by TryGet (responseType=Get) and TryExists (responseType=Exists).
///
/// A write can land on the same key between stage 1 (disk dispatch) and stage 3 (resume),
/// because the actor mailbox is free during stage 2. The reconciliation rule is:
///   — prefer the higher-revision resident entry;
///   — never blind-overwrite a resident entry with an older disk value.
/// After selecting the winning entry, apply the same state/expiry guard the inline read path
/// uses: a tombstone (State is Deleted/Undefined) or an expired entry resolves to DoesNotExist
/// rather than Get/Exists — even when the entry was inserted into the cache for future housekeeping.
///
/// The <see cref="responseType"/> determines the shape of the resolved response (Get or Exists),
/// which also drives the <c>isExists</c> flag in the PendingReads composite key so TryGet and
/// TryExists for the same key occupy separate slots and do not cross-coalesce.
///
/// All coalesced waiters (callers that attached to this in-flight read via AddWaiter) receive
/// the same resolved response via Resolve().
/// </summary>
internal sealed class PointReadContinuation : ReadContinuation
{
    private readonly string key;
    private readonly KeyValueResponseType responseType;

    internal PointReadContinuation(
        string key,
        KeyValueResponseType responseType,
        TaskCompletionSource<KeyValueResponse?> promise) : base(promise)
    {
        this.key = key;
        this.responseType = responseType;
    }

    internal override void Execute(KeyValueContext context)
    {
        // Remove before resolving so any new miss after this resume starts a fresh read.
        // isExists drives the third tuple element to match the registration key used in stage 1.
        bool isExists = responseType == KeyValueResponseType.Exists;
        context.PendingReads.Remove((key, -1L, isExists));

        if (Faulted)
        {
            Resolve(KeyValueStaticResponses.MustRetryResponse);
            return;
        }

        KeyValueEntry? disk = DiskResult;
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock
            .TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // Re-check resident store — a concurrent write may have landed while the disk read
        // was in flight. Prefer the higher revision; never corrupt the cache with a stale value.
        if (context.Store.TryGetValue(key, out KeyValueEntry? resident))
        {
            if (disk is null || resident.Revision >= disk.Revision)
            {
                context.TouchEntry(resident, currentTime);
                Resolve(ToResponse(resident, currentTime));
                return;
            }
        }

        // Disk entry is newer (or nothing was resident) — populate cache and serve disk value.
        if (disk is not null)
        {
            disk.FlushedRevision = disk.Revision;
            disk.LastUsed = currentTime;
            context.InsertStoreEntry(key, disk);
            Resolve(ToResponse(disk, currentTime));
        }
        else
        {
            Resolve(KeyValueStaticResponses.DoesNotExistContextResponse);
        }
    }

    private KeyValueResponse ToResponse(KeyValueEntry e, HLCTimestamp currentTime)
    {
        if (e.State is KeyValueState.Undefined or KeyValueState.Deleted
            || (e.Expires != HLCTimestamp.Zero && e.Expires - currentTime < TimeSpan.Zero))
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        return new(responseType, new ReadOnlyKeyValueEntry(
            responseType == KeyValueResponseType.Get ? e.Value : null,
            e.Revision, e.Expires, e.LastUsed, e.LastModified, e.State));
    }
}

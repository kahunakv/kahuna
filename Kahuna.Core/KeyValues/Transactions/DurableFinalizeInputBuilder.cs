
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Builds the frozen <see cref="DurableFinalizeInput"/> for the durable-intent path from a transaction's modified
/// keys and their staged committed values, grouping the prepared intents by their current data partition. Pure
/// and deterministic behind a <c>locate</c> seam so the freeze is unit-testable in isolation. Reads the session's
/// own staged-value and written-base dictionaries directly — the caller guarantees they cannot mutate during this
/// synchronous call (accepted operations have drained and the finalize owner fences new ones) and the returned
/// input owns every collection it carries, never a live view. Returns null — so the caller falls back to the
/// ticket path — when the transaction cannot be represented losslessly (not all-persistent, no anchor, or a
/// modified key with no staged value).
///
/// <para>Fidelity note: value/state/revision/expiry/<c>NoRevision</c> are exact — a <c>SET NOREV</c> materializes
/// revision-free on the durable path exactly as a direct write would. The mutation <see cref="KeyValueState"/> is
/// the state staged from the operation the caller issued — never derived from value presence, because a set may
/// carry a null value (the key exists and holds nothing) and must not finalize as a delete. The bucket is derived
/// from the key (its parent prefix) so it matches what the apply path recomputes. The validated base (<c>BaseRevision</c>/<c>BaseState</c>)
/// is the transaction's own pre-write read observation when one exists — exact, and enforced by the commit-time
/// staged-base compare-and-set — or the unknown-base sentinel for a blind write; it is never consulted for the
/// committed value (the materializer replays only value/revision/expiry/NoRevision).</para>
///
/// <para>Dispatch fences: each partition group carries one <see cref="RangeFence"/> per range descriptor its keys
/// resolved through (and one per hash-routed key space, at generation zero), each pairing a key with the
/// generation <b>that same key</b> resolved to. A partition whose keys span several key spaces — a key-range row
/// space beside a hash-routed meta space, or two key-range spaces at different generations — therefore fences
/// every space it touches, and never a key of one space against the generation of another: that mismatch read as
/// a moved range on every dispatch and refused the commit on every round.</para>
/// </summary>
internal static class DurableFinalizeInputBuilder
{
    public static bool TryBuild(
        HLCTimestamp transactionId,
        long epoch,
        string coordinatorKey,
        string anchorKey,
        HLCTimestamp commitTimestamp,
        HLCTimestamp decisionDeadline,
        IReadOnlyCollection<(string Key, KeyValueDurability Durability)> modifiedKeys,
        IReadOnlyDictionary<string, StagedValue> stagedByKey,
        Func<string, DurableKeyRoute> locate,
        out DurableFinalizeInput? input,
        IReadOnlyDictionary<(string Key, KeyValueDurability Durability), KeyValueTransactionReadKey>? writtenBases = null,
        string? origin = null)
    {
        input = null;

        if (modifiedKeys.Count == 0 || string.IsNullOrEmpty(anchorKey))
            return false;

        // Only all-persistent transactions are crash-atomic on this path; a mixed/ephemeral one keeps the ticket path.
        foreach ((string _, KeyValueDurability durability) in modifiedKeys)
            if (durability != KeyValueDurability.Persistent)
                return false;

        List<TransactionParticipantRef> manifest = new(modifiedKeys.Count);

        foreach ((string key, KeyValueDurability durability) in modifiedKeys)
            manifest.Add(new TransactionParticipantRef(key, durability));

        long manifestHash = TransactionManifest.ComputeHash(transactionId, epoch, anchorKey, commitTimestamp, manifest);

        Dictionary<int, List<PreparedIntent>> byPartition = [];
        Dictionary<int, List<RangeFence>> fencesByPartition = [];

        // One fence per distinct descriptor per partition. A descriptor is identified by its key space and start
        // bound; a hash-routed key space has no descriptor and dedupes on (space, null), so the whole space is
        // fenced once at generation zero.
        HashSet<FenceIdentity> fenced = [];

        foreach ((string key, KeyValueDurability _) in modifiedKeys)
        {
            if (!stagedByKey.TryGetValue(key, out StagedValue staged))
                return false; // a modified key with no staged value cannot be prepared losslessly — fall back.

            // The staged state is the operation the caller issued: a set with a null value (the key exists and
            // holds nothing) must not finalize as a delete, so value presence never decides it. Every staging
            // site records Set or Deleted; an Undefined state means the entry is not lossless, so fall back to
            // the ticket path, which commits the staged MVCC entries as the actors hold them.
            if (staged.State is not (KeyValueState.Set or KeyValueState.Deleted))
                return false;

            // Resolve the relative TTL to an absolute expiry anchored to the one canonical commit timestamp, so a
            // TTL write's expiry is deterministic across replicas and independent of any actor's wall clock.
            HLCTimestamp expires = staged.ExpiresMs > 0
                ? new HLCTimestamp(commitTimestamp.N, commitTimestamp.L + staged.ExpiresMs, commitTimestamp.C)
                : HLCTimestamp.Zero;

            // The validated base is the transaction's own pre-write read observation of this key, when it made
            // one: the exact committed revision/existence its read-modify-write is conditioned on, checked by
            // the commit-time staged-base compare-and-set. A key written blind (no prior read) has no base
            // dependency — last-writer-wins by design — and carries the unknown-base sentinel, which the check
            // skips. The staged revision is NOT a substitute (a mutation may bump it several times, or not at
            // all, relative to the committed head).
            long baseRevision = PreparedIntent.UnknownBaseRevision;
            KeyValueState baseState = KeyValueState.Undefined;
            // Every key on this path is persistent (checked above), so the persistent-durability entry is the
            // only observation that can bind to it.
            if (writtenBases is not null && writtenBases.TryGetValue((key, KeyValueDurability.Persistent), out KeyValueTransactionReadKey? observedBase))
            {
                baseRevision = observedBase.Revision;
                baseState = observedBase.Exists ? KeyValueState.Set : KeyValueState.Undefined;
            }

            string? bucket = GetBucket(key);

            PreparedIntent intent = new(
                transactionId, epoch, key, manifestHash, anchorKey, commitTimestamp,
                State: staged.State,
                Value: staged.Value,
                Bucket: bucket,
                Revision: staged.Revision,
                Expires: expires,
                NoRevision: staged.NoRevision,
                BaseRevision: baseRevision,
                BaseState: baseState,
                RecoveryDeadline: decisionDeadline,
                Resolution: PreparedIntentResolution.Pending,
                Origin: origin
            );

            DurableKeyRoute route = locate(key);

            if (!byPartition.TryGetValue(route.PartitionId, out List<PreparedIntent>? list))
            {
                byPartition[route.PartitionId] = list = [];
                fencesByPartition[route.PartitionId] = [];
            }

            list.Add(intent);

            // The bucket is the key's space (the prefix before the last '/'); a key with no '/' is its own space.
            AddFence(fencesByPartition[route.PartitionId], fenced, route.PartitionId, bucket ?? key, route, key);
        }

        DurableKeyRoute anchorRoute = locate(anchorKey);

        // The anchor partition's bundle carries the record initialization beside its prepare, so it is fenced
        // against the anchor descriptor as well; a partition the anchor key routes to but no written key does
        // is fenced by the record path alone.
        if (fencesByPartition.TryGetValue(anchorRoute.PartitionId, out List<RangeFence>? anchorFences))
            AddFence(anchorFences, fenced, anchorRoute.PartitionId, GetBucket(anchorKey) ?? anchorKey, anchorRoute, anchorKey);

        List<DurablePartitionPrepare> partitions = new(byPartition.Count);
        foreach ((int partitionId, List<PreparedIntent> intents) in byPartition)
            partitions.Add(new DurablePartitionPrepare(partitionId, fencesByPartition[partitionId], intents));

        input = new DurableFinalizeInput(
            transactionId, epoch, coordinatorKey, anchorKey, anchorRoute.PartitionId, anchorRoute.Generation,
            commitTimestamp, decisionDeadline, manifestHash, manifest, partitions, CreatedAt: transactionId);

        return true;
    }

    /// <summary>Adds <paramref name="key"/> at its own resolved generation as a fence of its partition, once per
    /// descriptor (or hash space) the partition's keys touch.</summary>
    private static void AddFence(List<RangeFence> fences, HashSet<FenceIdentity> fenced, int partitionId, string keySpace, DurableKeyRoute route, string key)
    {
        if (fenced.Add(new FenceIdentity(partitionId, keySpace, route.RangeStartKey)))
            fences.Add(new RangeFence(key, route.Generation));
    }

    /// <summary>The bucket (parent prefix) of a key, matching the derivation the actor apply path uses when it
    /// recomputes a fresh entry's bucket. Keeping the prepared intent's bucket consistent with that derivation
    /// keeps the intent faithful for bucket-scoped visibility and stable across a dedup-digest recompute. The
    /// bucket is also the key's routing space, so it names the descriptor a fence stands for.</summary>
    private static string? GetBucket(string key)
    {
        int index = key.LastIndexOf('/');
        return index == -1 ? null : key[..index];
    }

    /// <summary>What one dispatch fence stands for: a range descriptor (its key space and start bound) on a
    /// partition, or a hash-routed key space (null start bound) on it. Ordinal on every string.</summary>
    private readonly record struct FenceIdentity(int PartitionId, string KeySpace, string? RangeStartKey);
}

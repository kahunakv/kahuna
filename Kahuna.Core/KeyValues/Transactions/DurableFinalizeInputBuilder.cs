
using Kahuna.Server.KeyValues.Transactions.Data;
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
        Func<string, (int PartitionId, long Generation)> locate,
        out DurableFinalizeInput? input,
        IReadOnlyDictionary<(string Key, KeyValueDurability Durability), KeyValueTransactionReadKey>? writtenBases = null)
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
        Dictionary<int, long> generationByPartition = [];

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

            PreparedIntent intent = new(
                transactionId, epoch, key, manifestHash, anchorKey, commitTimestamp,
                State: staged.State,
                Value: staged.Value,
                Bucket: GetBucket(key),
                Revision: staged.Revision,
                Expires: expires,
                NoRevision: staged.NoRevision,
                BaseRevision: baseRevision,
                BaseState: baseState,
                RecoveryDeadline: decisionDeadline,
                Resolution: PreparedIntentResolution.Pending
            );

            (int partitionId, long generation) = locate(key);

            if (!byPartition.TryGetValue(partitionId, out List<PreparedIntent>? list))
                byPartition[partitionId] = list = [];

            list.Add(intent);
            generationByPartition[partitionId] = generation;
        }

        List<DurablePartitionPrepare> partitions = new(byPartition.Count);
        foreach ((int partitionId, List<PreparedIntent> intents) in byPartition)
            partitions.Add(new DurablePartitionPrepare(partitionId, generationByPartition[partitionId], intents));

        (int anchorPartitionId, long anchorGeneration) = locate(anchorKey);

        input = new DurableFinalizeInput(
            transactionId, epoch, coordinatorKey, anchorKey, anchorPartitionId, anchorGeneration,
            commitTimestamp, decisionDeadline, manifestHash, manifest, partitions, CreatedAt: transactionId);

        return true;
    }

    /// <summary>The bucket (parent prefix) of a key, matching the derivation the actor apply path uses when it
    /// recomputes a fresh entry's bucket. Keeping the prepared intent's bucket consistent with that derivation
    /// keeps the intent faithful for bucket-scoped visibility and stable across a dedup-digest recompute.</summary>
    private static string? GetBucket(string key)
    {
        int index = key.LastIndexOf('/');
        return index == -1 ? null : key[..index];
    }
}


using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The committed value staged for one modified key, the accurate part of the freeze available to the
/// coordinator (the mutation's value, revision, <b>relative</b> TTL in ms; 0 = no expiry, and the <c>NoRevision</c>
/// history-suppression flag). A null <see cref="Value"/> is a deletion. The relative TTL is resolved to an absolute
/// expiry HLC at freeze.</summary>
internal readonly record struct StagedMutation(byte[]? Value, long Revision, long ExpiresMs, bool NoRevision);

/// <summary>
/// Builds the frozen <see cref="DurableFinalizeInput"/> for the durable-intent path from a transaction's modified
/// keys and their staged committed values, grouping the prepared intents by their current data partition. Pure
/// and deterministic behind a <c>locate</c> seam so the freeze is unit-testable in isolation. Returns null — so
/// the caller falls back to the ticket path — when the transaction cannot be represented losslessly (not
/// all-persistent, no anchor, or a modified key with no staged value).
///
/// <para>Fidelity note: value/revision/expiry/<c>NoRevision</c> are exact — a <c>SET NOREV</c> materializes
/// revision-free on the durable path exactly as a direct write would. The mutation <see cref="KeyValueState"/> is
/// derived from value presence (a null value is a delete), and the bucket is derived from the key (its parent
/// prefix) so it matches what the apply path recomputes. Only the validated base (<c>BaseRevision</c>/
/// <c>BaseState</c>) is set best-effort — the authoritative pre-image lives in the owning actor's staged proposal
/// and would require an actor-side staging dispatch to source exactly; it is not consulted for the committed value
/// (the materializer replays only value/revision/expiry/NoRevision), only folded into the intent's dedup digest.</para>
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
        IReadOnlyDictionary<string, StagedMutation> stagedByKey,
        Func<string, (int PartitionId, long Generation)> locate,
        out DurableFinalizeInput? input)
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
            if (!stagedByKey.TryGetValue(key, out StagedMutation staged))
                return false; // a modified key with no staged value cannot be prepared losslessly — fall back.

            // Resolve the relative TTL to an absolute expiry anchored to the one canonical commit timestamp, so a
            // TTL write's expiry is deterministic across replicas and independent of any actor's wall clock.
            HLCTimestamp expires = staged.ExpiresMs > 0
                ? new HLCTimestamp(commitTimestamp.N, commitTimestamp.L + staged.ExpiresMs, commitTimestamp.C)
                : HLCTimestamp.Zero;

            PreparedIntent intent = new(
                transactionId, epoch, key, manifestHash, anchorKey, commitTimestamp,
                State: staged.Value is not null ? KeyValueState.Set : KeyValueState.Deleted,
                Value: staged.Value,
                Bucket: GetBucket(key),
                Revision: staged.Revision,
                Expires: expires,
                NoRevision: staged.NoRevision,
                BaseRevision: staged.Revision - 1,
                BaseState: KeyValueState.Set,
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

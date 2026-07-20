
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The committed value staged for one modified key, the accurate part of the freeze available to the
/// coordinator (the mutation's value, revision, and <b>relative</b> TTL in ms; 0 = no expiry). A null
/// <see cref="Value"/> is a deletion. The relative TTL is resolved to an absolute expiry HLC at freeze.</summary>
internal readonly record struct StagedMutation(byte[]? Value, long Revision, long ExpiresMs);

/// <summary>
/// Builds the frozen <see cref="DurableFinalizeInput"/> for the durable-intent path from a transaction's modified
/// keys and their staged committed values, grouping the prepared intents by their current data partition. Pure
/// and deterministic behind a <c>locate</c> seam so the freeze is unit-testable in isolation. Returns null — so
/// the caller falls back to the ticket path — when the transaction cannot be represented losslessly (not
/// all-persistent, no anchor, or a modified key with no staged value).
///
/// <para>Fidelity note: value/revision/expiry are exact; the mutation <see cref="KeyValueState"/> is derived from
/// value presence (a null value is a delete). <c>NoRevision</c> and the validated base are set best-effort — the
/// authoritative values live in the owning actor's staged proposal and would require an actor-side staging
/// dispatch to source exactly; these fields are not consulted for correctness on this path today.</para>
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
        Func<string, int> locate,
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
                Bucket: null,
                Revision: staged.Revision,
                Expires: expires,
                NoRevision: false,
                BaseRevision: staged.Revision - 1,
                BaseState: KeyValueState.Set,
                RecoveryDeadline: decisionDeadline,
                Resolution: PreparedIntentResolution.Pending
            );

            int partitionId = locate(key);

            if (!byPartition.TryGetValue(partitionId, out List<PreparedIntent>? list))
                byPartition[partitionId] = list = [];

            list.Add(intent);
        }

        List<DurablePartitionPrepare> partitions = new(byPartition.Count);
        foreach ((int partitionId, List<PreparedIntent> intents) in byPartition)
            partitions.Add(new DurablePartitionPrepare(partitionId, intents));

        input = new DurableFinalizeInput(
            transactionId, epoch, coordinatorKey, anchorKey, locate(anchorKey),
            commitTimestamp, decisionDeadline, manifestHash, manifest, partitions, CreatedAt: transactionId);

        return true;
    }
}

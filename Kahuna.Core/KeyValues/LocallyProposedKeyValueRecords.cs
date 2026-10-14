
using System.Diagnostics.CodeAnalysis;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The proposals behind the key/value log records this process serialized, keyed by the exact byte array handed
/// to Raft. A leader re-parses every record it commits — key string, value copy, byte-string wrapper — although
/// it still holds the <see cref="KeyValueProposal"/> the record was written from. The replicator takes the proposal
/// from here instead and builds the mutation from it; a miss (another process's record, WAL replay, state transfer)
/// parses as before. See <see cref="ProposedDeltaCache{TPayload}"/> for the reuse and lifetime contract. Reusing the
/// proposal is safe only because its fields, the value array included, are never written after it is created.
/// </summary>
internal static class LocallyProposedKeyValueRecords
{
    private static readonly ProposedDeltaCache<KeyValueProposal> proposals = new();

    /// <summary>
    /// Registers the proposal <paramref name="data"/> was serialized from. Only a value mutation whose logged type is
    /// the proposal's own type is registered: the apply path reads the type from the proposal, so a record logged
    /// under any other type must be parsed to be applied as logged.
    /// </summary>
    internal static void Register(byte[] data, KeyValueRequestType loggedType, KeyValueProposal proposal)
    {
        if (loggedType != proposal.Type)
            return;

        if (KeyValueMessageDecoder.Classify(loggedType) != KeyValueRecordKind.ValueMutation)
            return;

        proposals.Register(data, proposal);
    }

    /// <summary>Takes the proposal registered for <paramref name="data"/>, once per budgeted local apply.</summary>
    internal static bool TryTake(byte[] data, [NotNullWhen(true)] out KeyValueProposal? proposal) =>
        proposals.TryTake(data, out proposal);
}

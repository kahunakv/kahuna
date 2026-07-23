using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>What a write should do about a foreign durable prepared intent that covers the key it is mutating.</summary>
internal enum ForeignIntentWriteDecision
{
    /// <summary>No blocking intent (none, aborted, below-visibility, or already-settled): the write may proceed.
    /// Any committed-but-unsettled value has been materialized into the entry.</summary>
    Proceed,

    /// <summary>An undecided foreign intent covers the key: the write cannot replace it until the decision is known.</summary>
    MustRetry
}

/// <summary>
/// Deferred-settlement writer visibility (the write-side companion of <see cref="PreparedIntentVisibility"/>).
/// A committed transaction's value can linger in the partition's <see cref="Transactions.PreparedIntentStore"/>
/// before it materializes into local MVCC, so a write (set, delete, extend) must resolve the canonical outcome
/// before it may replace or act on the key:
/// <list type="bullet">
/// <item>committed intent — materialize it into the entry (creating and inserting a resident entry when none is
/// loaded, so an intent-only committed key is no longer seen as missing) so the write bases its next revision,
/// conditional flags, and existence checks on the committed value;</item>
/// <item>undecided intent — a live conflict; the caller must return <see cref="KeyValueResponseType.MustRetry"/>;</item>
/// <item>aborted or not-yet-visible intent — ignored; the write proceeds against the existing entry.</item>
/// </list>
/// The materialization is skipped when the entry already reflects a revision at or beyond the intent (settlement
/// already ran), so it never regresses a materialized entry. The intent belonging to this write's own transaction
/// is not foreign and is left to the ordinary MVCC path. A no-op (Proceed, entry untouched) when the durable-intent
/// store is absent, so all write paths are unchanged off the durable-intent path.
/// </summary>
internal static class ForeignIntentWriteResolver
{
    /// <summary>
    /// Resolves a durable intent owned by another transaction before a new write proceeds. A
    /// committed value is materialized through <paramref name="applyCommittedHead"/> so it cannot
    /// bypass revision archival; undecided intent ownership remains retryable.
    /// </summary>
    public static ForeignIntentWriteDecision Resolve(
        KeyValueContext context,
        string key,
        HLCTimestamp transactionId,
        ref KeyValueEntry? entry,
        Action<KeyValueEntry, KeyValueProposal, HLCTimestamp> applyCommittedHead,
        Transactions.Data.ForeignDecisionHint hint = default)
    {
        if (context.PreparedIntentStore?.Get(key) is not { } foreignIntent
            || foreignIntent.TransactionId == transactionId)
            return ForeignIntentWriteDecision.Proceed;

        switch (DurableReadVisibility.Resolve(context, foreignIntent, HLCTimestamp.Zero, hint))
        {
            case ReadVisibilityAction.Retry:
                return ForeignIntentWriteDecision.MustRetry;

            case ReadVisibilityAction.UseIntentValue:
                // Materialize the committed value into the entry unless the entry already reflects this intent. A
                // committed SET bumps the revision, so an unmaterialized entry is strictly behind (Revision <). A
                // committed DELETE keeps the revision, so an equal revision whose state still differs (entry Set,
                // intent Deleted) is an unmaterialized delete that must still be applied — otherwise a conditional
                // write such as SET NX would see the pre-delete value and wrongly reject as already-existing. Skip
                // only when the entry is beyond the intent, or already at its exact revision and terminal state.
                if (entry is null
                    || entry.Revision < foreignIntent.Revision
                    || (entry.Revision == foreignIntent.Revision && entry.State != foreignIntent.State))
                {
                    bool created = entry is null;
                    entry ??= new()
                    {
                        // The intent already carries this key's bucket (derived from the same key at freeze), so
                        // reuse it instead of re-deriving the prefix string.
                        Bucket = foreignIntent.Bucket,
                        Revision = -1,
                        State = KeyValueState.Undefined
                    };

                    KeyValueProposal proposal = new(
                        foreignIntent.State == KeyValueState.Deleted
                            ? KeyValueRequestType.TryDelete
                            : KeyValueRequestType.TrySet,
                        key,
                        foreignIntent.Value,
                        foreignIntent.Revision,
                        foreignIntent.NoRevision,
                        foreignIntent.Expires,
                        foreignIntent.CommitTimestamp,
                        foreignIntent.CommitTimestamp,
                        foreignIntent.State,
                        KeyValueDurability.Persistent);

                    applyCommittedHead(entry, proposal, foreignIntent.TransactionId);
                    entry.FlushedRevision = foreignIntent.Revision;

                    if (created)
                        context.InsertStoreEntry(key, entry);
                }

                return ForeignIntentWriteDecision.Proceed;

            case ReadVisibilityAction.UseExisting:
            default:
                return ForeignIntentWriteDecision.Proceed;
        }
    }
}

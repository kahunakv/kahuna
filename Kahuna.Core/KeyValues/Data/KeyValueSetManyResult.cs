using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The answer for one item of a batched transactional set, at the item's position in the request.
/// <para><see cref="Type"/>, <see cref="Revision"/> and <see cref="LastModified"/> mean exactly what a single
/// transactional <c>TrySet</c> of the key answers. <see cref="KeyValueResponseType.NotApplied"/> is the answer of an
/// item that passed its own condition and conflicts but was not staged, because the batch stages every item or none
/// and another item was refused or transient, or because the request was a check round.</para>
/// <para><see cref="HolderTransactionId"/> names the transaction whose intent or lock a <see cref="KeyValueResponseType.MustRetry"/>
/// or <see cref="KeyValueResponseType.WaitingForReplication"/> item ran into; Zero when no holder applies.</para>
/// </summary>
public readonly record struct KeyValueSetManyResult(
    KeyValueResponseType Type,
    long Revision,
    HLCTimestamp LastModified,
    HLCTimestamp HolderTransactionId)
{
    /// <summary>An item that passed its checks but was not staged.</summary>
    public static KeyValueSetManyResult NotApplied => new(KeyValueResponseType.NotApplied, -1, HLCTimestamp.Zero, HLCTimestamp.Zero);

    /// <summary>The answer a single transactional set gave, carried at the item's position.</summary>
    public static KeyValueSetManyResult From(KeyValueResponse response) =>
        new(response.Type, response.Revision, response.Ticket, response.HolderTransactionId);
}

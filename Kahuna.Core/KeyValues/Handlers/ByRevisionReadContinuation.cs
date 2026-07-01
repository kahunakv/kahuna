
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stage-3 resolution for a persistent by-revision read (TryGet or TryExists) whose target
/// revision was not found in the in-memory cache or the entry's Revisions dictionary.
///
/// Historical revisions are immutable once written, so stage 3 performs no resident
/// reconciliation: the raw disk result is served directly.
///
/// The <see cref="responseType"/> carries the shape of this specific continuation (Get or
/// Exists). TryGet and TryExists for the same (key, revision) must NOT share a continuation
/// because the responseType is fixed at construction: a Get continuation would deliver a null
/// value to an Exists caller, and an Exists continuation would suppress the value for a Get
/// caller. The coalescing key in PendingReads includes the shape flag (<c>isExists</c>) so the
/// two operations occupy separate slots and coalescing only happens within the same shape.
/// </summary>
internal sealed class ByRevisionReadContinuation : ReadContinuation
{
    private readonly string key;
    private readonly long revision;
    private readonly KeyValueResponseType responseType;

    internal ByRevisionReadContinuation(
        string key,
        long revision,
        KeyValueResponseType responseType,
        TaskCompletionSource<KeyValueResponse?> promise) : base(promise)
    {
        this.key = key;
        this.revision = revision;
        this.responseType = responseType;
    }

    internal override void RemovePendingKey(KeyValueContext context)
    {
        // The PendingReads key includes isExists to keep TryGet and TryExists separate.
        context.PendingReads.Remove((key, revision, responseType == KeyValueResponseType.Exists));
    }

    internal override void Execute(KeyValueContext context)
    {
        RemovePendingKey(context);

        if (Faulted)
        {
            Resolve(KeyValueStaticResponses.MustRetryResponse);
            return;
        }

        if (DiskResult is null)
        {
            Resolve(KeyValueStaticResponses.DoesNotExistContextResponse);
            return;
        }

        // Historical revision is immutable — serve the disk row directly without merging against
        // the current resident store. Value is included for Get, suppressed for Exists.
        Resolve(new(responseType, new ReadOnlyKeyValueEntry(
            responseType == KeyValueResponseType.Get ? DiskResult.Value : null,
            revision,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            KeyValueState.Set)));
    }
}

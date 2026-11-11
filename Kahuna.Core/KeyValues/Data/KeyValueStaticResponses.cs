
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Provides a set of predefined static responses for common outcomes when interacting with key-value operations.
/// </summary>
/// <remarks>
/// This class is used as a utility to provide standardized, reusable responses
/// for various scenarios, such as when a key does not exist, when an error occurs,
/// or when locks are acquired or already held.
/// </remarks>
internal static class KeyValueStaticResponses
{
    internal static readonly KeyValueResponse WaitingForReplicationResponse = new(KeyValueResponseType.WaitingForReplication);
    
    internal static readonly KeyValueResponse DoesNotExistResponse = new(KeyValueResponseType.DoesNotExist);
    
    internal static readonly KeyValueResponse ErroredResponse = new(KeyValueResponseType.Errored);
    
    internal static readonly KeyValueResponse LockedResponse = new(KeyValueResponseType.Locked);
    
    internal static readonly KeyValueResponse UnlockedResponse = new(KeyValueResponseType.Unlocked);

    internal static readonly KeyValueResponse AlreadyLockedResponse = new(KeyValueResponseType.AlreadyLocked);
    
    internal static readonly KeyValueResponse AbortedResponse = new(KeyValueResponseType.Aborted);

    internal static readonly KeyValueResponse PrepareResponse = new(KeyValueResponseType.Prepared);
    
    internal static readonly KeyValueResponse MustRetryResponse = new(KeyValueResponseType.MustRetry);

    /// <summary>The store reported damage, or is fail-stopped on it: terminal for this node, never retried.</summary>
    internal static readonly KeyValueResponse StorageCorruptResponse = new(KeyValueResponseType.StorageCorrupt);

    /// <summary>A snapshot read below the history this node still keeps: not retryable at the same timestamp.</summary>
    internal static readonly KeyValueResponse SnapshotTooOldResponse = new(KeyValueResponseType.SnapshotTooOld);

    /// <summary>MustRetry from a release that may only acknowledge what is held and found nothing of the
    /// transaction; see <see cref="KeyValueResponse.NothingHeld"/>.</summary>
    internal static readonly KeyValueResponse NothingHeldResponse = KeyValueResponse.NothingHeldHere();

    internal static readonly KeyValueResponse NotSetResponse = new(KeyValueResponseType.NotSet);

    internal static readonly KeyValueResponse InvalidInputResponse = new(KeyValueResponseType.InvalidInput);

    internal static readonly KeyValueResponse DoesNotExistContextResponse = new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueEntry(null, 0, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Undefined));
}
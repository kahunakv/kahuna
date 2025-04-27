
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
    internal static readonly KeyValueResponse DoesNotExistResponse = new(KeyValueResponseType.DoesNotExist);
    
    internal static readonly KeyValueResponse ErroredResponse = new(KeyValueResponseType.Errored);
    
    internal static readonly KeyValueResponse LockedResponse = new(KeyValueResponseType.Locked);
    
    internal static readonly KeyValueResponse UnlockedResponse = new(KeyValueResponseType.Unlocked);

    internal static readonly KeyValueResponse AlreadyLockedResponse = new(KeyValueResponseType.AlreadyLocked);
    
    internal static readonly KeyValueResponse AbortedResponse = new(KeyValueResponseType.Aborted);

    internal static readonly KeyValueResponse DoesNotExistContextResponse = new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueContext(null, 0, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Undefined));
}

using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

internal static class KeyValueStaticResponses
{
    internal static readonly KeyValueResponse DoesNotExistResponse = new(KeyValueResponseType.DoesNotExist);
    
    internal static readonly KeyValueResponse ErroredResponse = new(KeyValueResponseType.Errored);
    
    internal static readonly KeyValueResponse LockedResponse = new(KeyValueResponseType.Locked);

    internal static readonly KeyValueResponse AlreadyLockedResponse = new(KeyValueResponseType.AlreadyLocked);

    internal static readonly KeyValueResponse DoesNotExistContextResponse = new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueContext(null, 0, HLCTimestamp.Zero, KeyValueState.Undefined));
}
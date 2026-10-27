
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The answer of a lock release served on a node whose leadership no quorum confirmed. Such a release
/// acknowledges only what the node holds of the transaction (<see cref="KeyValueRequest.ReleaseOnlyIfHeld"/>).
/// <see cref="NothingHeld"/> tells the locator that the node found nothing of the transaction, so the lock may
/// live on another replica and the release must travel on; every other <see cref="KeyValueResponseType.MustRetry"/>
/// is a transient condition of this node and is answered as it is.
/// </summary>
internal readonly record struct HeldHereRelease(KeyValueResponseType Type, bool NothingHeld);

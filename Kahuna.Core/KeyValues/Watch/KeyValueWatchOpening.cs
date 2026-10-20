using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// The answer to a watch request. <see cref="Type"/> is <see cref="KeyValueResponseType.Get"/> with an open
/// <see cref="Watch"/>, or a refusal with no watch. A <see cref="KeyValueResponseType.DoesNotExist"/> refusal
/// carries <see cref="Routes"/>: where each partition of the scope is hosted, so the caller can open the watch
/// on nodes that host it.
/// </summary>
public readonly record struct KeyValueWatchOpening(KeyValueResponseType Type, KeyValueWatch? Watch, IReadOnlyList<KeyValueWatchRoute> Routes)
{
    public KeyValueWatchOpening(KeyValueResponseType type, KeyValueWatch? watch) : this(type, watch, [])
    {
    }

    public void Deconstruct(out KeyValueResponseType type, out KeyValueWatch? watch)
    {
        type = Type;
        watch = Watch;
    }
}

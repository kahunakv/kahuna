using Kahuna.Shared.KeyValue;

namespace Kahuna;

/// <summary>
/// Thrown when a key belongs to a key space that this node routes by key range, and no range descriptor covers
/// the key in the node's current range map. A validated range map has no gaps, so this names a transient state of
/// the node, not of the key: the space was registered before its seed descriptor replicated here, or its descriptors
/// were removed and the routing mode has not been reconciled yet. Nothing was performed, so the request is
/// retryable: the exception carries <see cref="KeyValueResponseType.MustRetry"/>, the public surfaces answer it as
/// MustRetry, and an embedded caller can classify it through <see cref="KahunaServerException.ResponseType"/>.
/// </summary>
public sealed class RangeDescriptorMissingException(string key, string keySpace) : KahunaServerException(
    $"No range descriptor covers key '{key}' in key-range space '{keySpace}'.", KeyValueResponseType.MustRetry)
{
    /// <summary>The key no descriptor covered.</summary>
    public string Key { get; } = key;

    /// <summary>The key-range space the key belongs to.</summary>
    public string KeySpace { get; } = keySpace;
}

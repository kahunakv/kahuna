
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents the result of a paginated range scan over key-value entries.
/// </summary>
/// <remarks>
/// Placement note: Task 3 originally suggested <c>Kahuna.Shared/KeyValue/</c>, but this type
/// lives here alongside <see cref="KeyValueGetByBucketResult"/> because it is server-internal.
/// The gRPC boundary uses proto-generated <c>GrpcGetByRangeResponse</c> / <c>GrpcGetByRangePageResponse</c>
/// for cross-node transport; <see cref="KeyValueGetByRangeResult"/> is never serialised directly
/// and does not need to be part of the shared client contract.
/// </remarks>
public sealed class KeyValueGetByRangeResult
{
    public KeyValueResponseType Type { get; }

    public List<(string, ReadOnlyKeyValueEntry)> Items { get; }

    /// <summary>
    /// Opaque cursor that encodes the resume position for the next page.
    /// Null when <see cref="HasMore"/> is false.
    /// </summary>
    public string? NextCursor { get; }

    /// <summary>
    /// True when the result was capped by the page limit and more items remain.
    /// </summary>
    public bool HasMore { get; }

    public KeyValueGetByRangeResult(
        KeyValueResponseType type,
        List<(string, ReadOnlyKeyValueEntry)> items,
        string? nextCursor,
        bool hasMore)
    {
        Type = type;
        Items = items;
        NextCursor = nextCursor;
        HasMore = hasMore;
    }
}

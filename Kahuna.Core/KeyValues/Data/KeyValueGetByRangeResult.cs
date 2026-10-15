
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents the result of a paginated range scan over key-value entries.
/// </summary>
/// <remarks>
/// Placement note: this type
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

    /// <summary>
    /// What the page cost the serving node (see <see cref="KeyValueScanObservation"/>). Null when it is
    /// unknown: a peer on an older build served the page, or the page never reached a scan handler (a
    /// refusal before dispatch). Null never means zero.
    /// </summary>
    public KeyValueScanObservation? Observation { get; internal set; }

    public KeyValueGetByRangeResult(
        KeyValueResponseType type,
        List<(string, ReadOnlyKeyValueEntry)> items,
        string? nextCursor,
        bool hasMore,
        KeyValueScanObservation? observation = null)
    {
        Type = type;
        Items = items;
        NextCursor = nextCursor;
        HasMore = hasMore;
        Observation = observation;
    }
}

using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Names the key space a range-administration call acts on: <c>POST /v1/ranges/register</c> and
/// <c>POST /v1/ranges/unregister</c>.
/// </summary>
public sealed class KahunaKeyRangeRequest
{
    /// <summary>
    /// The key space — a key's prefix up to (excluding) its last <c>'/'</c>, so the key
    /// <c>jepsen/register/4</c> lives in the key space <c>jepsen/register</c>. Spaces ending in
    /// <c>/meta</c> are schema-log spaces and are refused.
    /// </summary>
    [JsonPropertyName("keySpace")]
    public string KeySpace { get; set; } = "";
}

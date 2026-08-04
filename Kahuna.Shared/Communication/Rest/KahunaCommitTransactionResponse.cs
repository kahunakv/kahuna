
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaCommitTransactionResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    /// <summary>
    /// The coordinator's canonical record anchor, or null when the transaction wrote nothing persistent.
    /// </summary>
    [JsonPropertyName("recordAnchorKey")]
    public string? RecordAnchorKey { get; set; }
}

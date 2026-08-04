
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Outcome of a key-value lock acquire or release. <see cref="HolderTransactionId"/> identifies the
/// current holder when acquisition was refused; it is zero when the lock has no competing holder.
/// </summary>
public sealed class KahunaKeyValueLockResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("holderTransactionId")]
    public HLCTimestamp HolderTransactionId { get; set; }
}

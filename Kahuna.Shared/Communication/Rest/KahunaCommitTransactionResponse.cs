
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

    /// <summary>
    /// Why the commit was refused when <see cref="Type"/> is <see cref="KeyValueResponseType.Aborted"/> or
    /// <see cref="KeyValueResponseType.MustRetry"/>; <see cref="CommitRefusalClass.None"/> otherwise.
    /// </summary>
    [JsonPropertyName("refusal")]
    public CommitRefusalClass Refusal { get; set; }

    /// <summary>The coordinator's reason for the refusal, when it gave one.</summary>
    /// <summary>On a commit, the transaction's commit timestamp; zero when none applies.</summary>
    [JsonPropertyName("commitTimestamp")]
    public Kommander.Time.HLCTimestamp CommitTimestamp { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}


using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Commits or rolls back an interactive transaction session. <see cref="RecordAnchorKey"/> carries the
/// coordinator's canonical anchor so a retry that outlived the coordinating session can still reach the
/// durable decision instead of reporting an unknown outcome.
/// </summary>
public sealed class KahunaCommitTransactionRequest
{
    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("recordAnchorKey")]
    public string? RecordAnchorKey { get; set; }
}

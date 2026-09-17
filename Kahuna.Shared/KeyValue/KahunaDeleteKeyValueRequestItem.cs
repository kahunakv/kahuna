using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaDeleteKeyValueRequestItem
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }

    /// <summary>
    /// Conflict policy of the owning session, stamped by the node that registered the batch with the
    /// session's coordinator and carried on the inter-node forward. A client-supplied value is ignored on a
    /// registered batch; it is honoured on an unregistered batch only when the batch arrived from a peer.
    /// </summary>
    [JsonPropertyName("conflictPolicy")]
    public TransactionConflictPolicy ConflictPolicy { get; set; }
}

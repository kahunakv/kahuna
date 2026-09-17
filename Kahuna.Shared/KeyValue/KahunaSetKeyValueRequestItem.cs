using System.Text.Json.Serialization;
using Kahuna.Shared.Communication.Rest;
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaSetKeyValueRequestItem
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }
    
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("value")]
    [JsonConverter(typeof(KeyValuePayloadJsonConverter))]
    public byte[]? Value { get; set; }

    [JsonPropertyName("compareValue")]
    [JsonConverter(typeof(KeyValuePayloadJsonConverter))]
    public byte[]? CompareValue { get; set; }
    
    [JsonPropertyName("compareRevision")]
    public long CompareRevision { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }
    
    [JsonPropertyName("flags")]
    public KeyValueFlags Flags { get; set; }
    
    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }

    [JsonPropertyName("routedGeneration")]
    public long RoutedGeneration { get; set; }

    /// <summary>
    /// Conflict policy of the owning session, stamped by the node that registered the batch with the
    /// session's coordinator and carried on the inter-node forward. A client-supplied value is ignored on a
    /// registered batch; it is honoured on an unregistered batch only when the batch arrived from a peer.
    /// </summary>
    [JsonPropertyName("conflictPolicy")]
    public TransactionConflictPolicy ConflictPolicy { get; set; }
}
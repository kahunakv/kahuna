using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// One returned value of a transaction script execution: the key it belongs to and the
/// value/revision/timestamps of the affected entry, mirroring the gRPC script wire so REST
/// clients see the same per-value fidelity.
/// </summary>
public sealed class KahunaTxKeyValueResponseItem
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("value")]
    public byte[]? Value { get; set; }

    [JsonPropertyName("revision")]
    public long Revision { get; set; }

    [JsonPropertyName("expires")]
    public HLCTimestamp Expires { get; set; }

    [JsonPropertyName("lastModified")]
    public HLCTimestamp LastModified { get; set; }
}

public sealed class KahunaTxKeyValueResponse
{
    public string? ServedFrom { get; set; }

    public KeyValueResponseType Type { get; set; }

    public byte[]? Value { get; set; }

    public long Revision { get; set; }

    public HLCTimestamp Expires { get; set; }

    public string? Reason { get; set; }

    /// <summary>Per-value results of the script, in execution-result order. The legacy
    /// scalar fields above carry the first value for older consumers.</summary>
    public List<KahunaTxKeyValueResponseItem>? Values { get; set; }
}

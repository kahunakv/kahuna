using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaTxKeyValueResponse
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
}
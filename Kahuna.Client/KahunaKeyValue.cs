
using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

public class KahunaKeyValue
{
    private readonly KahunaClient client;
    
    private readonly string key;

    private readonly KeyValueConsistency consistency;
    
    public bool Success { get; }
    
    public long Revision { get; }
    
    public byte[]? Value { get; }

    public KahunaKeyValue(KahunaClient client, string key, bool success, long revision, KeyValueConsistency consistency)
    {
        this.client = client;
        this.key = key;
        Success = success;
        Revision = revision;
        this.consistency = consistency;
    }
    
    public KahunaKeyValue(KahunaClient client, string key, bool success, byte[]? value, long revision, KeyValueConsistency consistency)
    {
        this.client = client;
        this.key = key;
        Success = success;
        Value = value;
        Revision = revision;
        this.consistency = consistency;
    }
    
    public string? ValueAsString()
    {
        return Value is null ? null : Encoding.UTF8.GetString(Value);
    }
}

using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

public class KahunaKeyValue
{
    private readonly KahunaClient client;
    
    private readonly string key;

    private readonly KeyValueDurability durability;
    
    public bool Success { get; }
    
    public long Revision { get; }
    
    public byte[]? Value { get; }

    public KahunaKeyValue(KahunaClient client, string key, bool success, long revision, KeyValueDurability durability)
    {
        this.client = client;
        this.key = key;
        Success = success;
        Revision = revision;
        this.durability = durability;
    }
    
    public KahunaKeyValue(KahunaClient client, string key, bool success, byte[]? value, long revision, KeyValueDurability durability)
    {
        this.client = client;
        this.key = key;
        Success = success;
        Value = value;
        Revision = revision;
        this.durability = durability;
    }
    
    public string? ValueAsString()
    {
        return Value is null ? null : Encoding.UTF8.GetString(Value);
    }
}
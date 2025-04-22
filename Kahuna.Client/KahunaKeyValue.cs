
using System.Text;
using System.Text.Json;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

public class KahunaKeyValue
{
    private static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new() { WriteIndented = false };
    
    private readonly KahunaClient client;
    
    private readonly string key;

    private readonly KeyValueDurability durability;
    
    public bool Success { get; }
    
    public long Revision { get; }
    
    public byte[]? Value { get; }
    
    public int TimeElapsedMs { get; }

    public KahunaKeyValue(KahunaClient client, string key, bool success, long revision, KeyValueDurability durability, int timeElapsedMs)
    {
        this.client = client;
        this.key = key;
        Success = success;
        Revision = revision;
        this.durability = durability;
        TimeElapsedMs = timeElapsedMs;
    }
    
    public KahunaKeyValue(KahunaClient client, string key, bool success, byte[]? value, long revision, KeyValueDurability durability, int timeElapsedMs)
    {
        this.client = client;
        this.key = key;
        Success = success;
        Value = value;
        Revision = revision;
        this.durability = durability;
        TimeElapsedMs = timeElapsedMs;
    }
    
    public string? ValueAsString()
    {
        return Value is null ? null : Encoding.UTF8.GetString(Value);
    }
    
    /// <summary>
    /// Try to extend the expiration of the key by the specified duration.
    /// Returns true if the key expiration was successfully extended, false otherwise. 
    /// </summary>
    /// <param name="duration"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaKeyValue> Extend(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        //if (string.IsNullOrEmpty(servedFrom) || !client.UpgradeUrls)
        return await client.ExtendKeyValue(key, duration, durability, cancellationToken);
        
        //return await client.Communication.TryExtend(servedFrom, resource, owner, (int)duration.TotalMilliseconds, durability, cancellationToken);
    }
    
    /// <summary>
    /// Try to delete the key
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaKeyValue> Delete(CancellationToken cancellationToken = default)
    {
        //if (string.IsNullOrEmpty(servedFrom) || !client.UpgradeUrls)
            return await client.DeleteKeyValue(key, durability, cancellationToken);
        
        //return await client.Communication.TryExtend(servedFrom, resource, owner, (int)duration.TotalMilliseconds, durability, cancellationToken);
    }

    /// <summary>
    /// Returns the current key/value state as a JSON string.
    /// </summary>
    /// <returns></returns>
    public string ToJson()
    {
        return JsonSerializer.Serialize(new
        {
            key,
            success = Success,
            revision = Revision,
            value = ValueAsString(),
            durability = durability.ToString()
        }, DefaultJsonSerializerOptions);
    }
}
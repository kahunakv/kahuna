
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

/// <summary>
/// Represents a key-value pair within the Kahuna client system. Provides methods to access value information,
/// manage key-value lifecycle operations, and encapsulate metadata about the operation state and configuration.
/// </summary>
public class KahunaKeyValue
{
    private static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new() { WriteIndented = false };
    
    private readonly KahunaClient client;

    private readonly KeyValueDurability durability;
    
    public string Key { get; }

    /// <summary>
    /// Indicates whether the operation was successful.
    /// </summary>
    public bool Success { get; }

    /// <summary>
    /// Represents the revision number associated with the key-value pair.
    /// </summary>
    public long Revision { get; }

    /// <summary>
    /// Represents the stored value associated with the key.
    /// </summary>
    public byte[]? Value { get; }

    /// <summary>
    /// Gets the time, in milliseconds, that elapsed during the operation.
    /// </summary>
    public int TimeElapsedMs { get; }

    /// <summary>
    /// Represents a key-value operation result, encapsulating details such as success status, revision number,
    /// durability type, and time elapsed during the operation.
    /// </summary>
    public KahunaKeyValue(KahunaClient client, string key, bool success, long revision, KeyValueDurability durability, int timeElapsedMs)
    {
        this.client = client;
        this.Key = key;
        Success = success;
        Revision = revision;
        this.durability = durability;
        TimeElapsedMs = timeElapsedMs;
    }

    /// <summary>
    /// Represents a key-value object associated with the KahunaClient, containing properties
    /// such as the operation's success status, key, value, revision number, durability level,
    /// and the time taken to complete the operation.
    /// </summary>
    public KahunaKeyValue(KahunaClient client, string key, bool success, byte[]? value, long revision, KeyValueDurability durability, int timeElapsedMs)
    {
        this.client = client;
        this.Key = key;
        Success = success;
        Value = value;
        Revision = revision;
        this.durability = durability;
        TimeElapsedMs = timeElapsedMs;
    }

    /// <summary>
    /// Converts the stored binary value to its string representation using UTF-8 encoding.
    /// Returns null if the value is not set.
    /// </summary>
    /// <returns>A string representation of the binary value, or null if the value is unavailable.</returns>
    public string? ValueAsString()
    {
        return Value is null ? null : Encoding.UTF8.GetString(Value);
    }
    
    /// <summary>
    /// Converts the stored binary value to its long representation
    /// Throws exception if the value is not set or cannot be parsed as a long.
    /// </summary>
    /// <returns>A string representation of the binary value, or null if the value is unavailable.</returns>
    public long ValueAsLong()
    {
        if (Value is null || !long.TryParse(Encoding.ASCII.GetString(Value), out long result))
            throw new KahunaException("Value cannot be casted to long", KeyValueResponseType.InvalidInput);

        return result;
    }
    
    /// <summary>
    /// Converts the stored binary value to its long representation
    /// Throws exception if the value is not set or cannot be parsed as a long.
    /// </summary>
    /// <returns>A string representation of the binary value, or null if the value is unavailable.</returns>
    public bool ValueAsBool()
    {
        if (Value is null || !bool.TryParse(Encoding.ASCII.GetString(Value), out bool result))
            throw new KahunaException("Value cannot be casted to long", KeyValueResponseType.InvalidInput);

        return result;
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
        return await client.ExtendKeyValue(Key, duration, durability, cancellationToken);
        
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
            return await client.DeleteKeyValue(Key, durability, cancellationToken);
        
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
            Key,
            success = Success,
            revision = Revision,
            value = ValueAsString(),
            durability = durability.ToString()
        }, DefaultJsonSerializerOptions);
    }
}
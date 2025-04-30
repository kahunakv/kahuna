
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

public class KahunaKeyValueTransactionResultValue
{
    public string? Key { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public HLCTimestamp LastModified { get; set; }
}

/// <summary>
/// Represents the result of a key-value transaction operation in the Kahuna system.
/// </summary>
public class KahunaKeyValueTransactionResult
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
       
    public List<KahunaKeyValueTransactionResultValue>? Values { get; set; }
    
    public int TimeElapsedMs { get; set; }
    
    public long FirstRevision
    {
        get
        {
            if (Values is null || Values.Count == 0)
                return 0;

            return Values[0].Revision;
        }
    }
    
    public byte[]? FirstValue
    {
        get
        {
            if (Values is null || Values.Count == 0)
                return null;

            return Values[0].Value;
        }
    }
    
    public string? FirstValueAsString
    {
        get
        {
            if (Values is null || Values.Count == 0)
                return null;

            byte[]? rawValue = Values[0].Value;

            if (rawValue is null)
                return null;
            
            return Encoding.UTF8.GetString(rawValue);
        }
    }
}
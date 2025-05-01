
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Client;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Control.Commands;

/// <summary>
/// Provides a command to retrieve key-value pairs from a Kahuna storage system based on a specified prefix.
/// This class encapsulates the functionality for executing a "GetByBucket" operation against a given
/// <see cref="KahunaClient"/> connection.
/// </summary>
public static class KeyValueGetByBucketCommand
{    
    public static async Task Execute(KahunaClient connection, string optsGet, string? format)
    {                
        List<KahunaKeyValue> kv = await connection.GetByBucket(
            optsGet,
            KeyValueDurability.Persistent, 
            CancellationToken.None
        );       

        if (format == "json")
            Console.WriteLine("{0}", "-");
        else
        {
            foreach (var item in kv)
                Console.WriteLine("r{0} {1} {2}", item.Revision, item.Key, item.ValueAsString() ?? "-");
        }
    }
}
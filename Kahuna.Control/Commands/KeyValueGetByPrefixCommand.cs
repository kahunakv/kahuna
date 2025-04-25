
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
/// This class encapsulates the functionality for executing a "GetByPrefix" operation against a given
/// <see cref="KahunaClient"/> connection.
/// </summary>
public static class KeyValueGetByPrefixCommand
{    
    public static async Task Execute(KahunaClient connection, string optsGet, string? format)
    {                
        (bool success, List<string> items) = await connection.GetByPrefix(
            optsGet,
            KeyValueDurability.Persistent, 
            CancellationToken.None
        );       

        if (format == "json")
            Console.WriteLine("{0}", "-");
        else
        {
            foreach (string item in items)
                Console.WriteLine("r{0} {1} {2}", 0, item, "-");
        }
    }
}
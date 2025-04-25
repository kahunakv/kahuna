
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
/// Represents a command for retrieving a key-value pair using the specified connection and options.
/// </summary>
public static class KeyValueGetCommand
{    
    public static async Task Execute(KahunaClient connection, string optsGet, string? format)
    {                
        KahunaKeyValue result = await connection.GetKeyValue(
            optsGet,
            KeyValueDurability.Persistent, 
            CancellationToken.None
        );       

        if (format == "json")
            Console.WriteLine("{0}", result.ToJson());
        else
            Console.WriteLine("r{0} {1} {2}", result.Revision, result.Success ? "get" : "not get", result.ValueAsString());
    }
}
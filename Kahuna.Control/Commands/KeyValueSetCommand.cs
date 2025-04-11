
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Control.Commands;

public static class KeyValueSetCommand
{    
    public static async Task Execute(KahunaClient connection, string optsSet, string? optsValue, string? format)
    {
        KahunaKeyValue result = await connection.SetKeyValue(
            optsSet, 
            Encoding.UTF8.GetBytes(optsValue!), 
            0,
            KeyValueFlags.Set, 
            KeyValueDurability.Persistent, 
            CancellationToken.None
        );

        if (format == "json")
            Console.WriteLine("{0}", result.ToJson());
        else
            Console.WriteLine("r{0} {1}", result.Revision, result.Success ? "set" : "not set");
    }
}
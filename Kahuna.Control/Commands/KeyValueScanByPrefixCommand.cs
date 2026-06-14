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
/// Provides a command to scan key-value pairs from a Kahuna storage system based on a specified prefix.
/// </summary>
public static class KeyValueScanByPrefixCommand
{
    public static async Task Execute(KahunaClient connection, string prefix, string? format)
    {
        List<KahunaKeyValue> kv = await connection.ScanAllByPrefix(
            prefix,
            KeyValueDurability.Persistent,
            cancellationToken: CancellationToken.None
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


/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Client;

namespace Kahuna.Control.Commands;

public static class LockCommand
{    
    public static async Task Execute(KahunaClient connection, string optsLock, string? format)
    {
        KahunaLock result = await connection.GetOrCreateLock(optsLock, TimeSpan.FromMilliseconds(3000));

        if (format == "json")
            Console.WriteLine("{0}", result.ToJson());
        else
            Console.WriteLine("f{0} {1} {2}", result.FencingToken, result.IsAcquired ? "acquired" : "not acquired", result.OwnerAsString);
    }
}
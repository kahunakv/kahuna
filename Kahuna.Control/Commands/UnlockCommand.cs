/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Client;
using Kahuna.Shared.Locks;

namespace Kahuna.Control.Commands;

public static class UnlockCommand
{    
    public static async Task Execute(KahunaClient connection, string optsUnlock, string optsOwner, string? format)
    {
        bool result = await connection.Unlock(optsUnlock, optsOwner, LockDurability.Persistent);

        if (format == "json")
            Console.WriteLine("{0}", result.ToString());
        else
            Console.WriteLine("{0}", result ? "unlocked" : "not unlocked");
    }
}
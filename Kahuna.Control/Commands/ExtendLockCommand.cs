
using Kahuna.Client;

namespace Kahuna.Control.Commands;

/// <summary>
/// The <c>ExtendLockCommand</c> class provides functionality to extend an existing lock
/// managed by a <see cref="KahunaClient"/>. It interacts with the lock system to
/// attempt an extension of the lock's expiration time, ensuring that the lock remains valid
/// for the specified duration.
/// </summary>
public static class ExtendLockCommand
{    
    public static async Task Execute(KahunaClient connection, string optsLock, string optsOwner, int optsExpires, string? format)
    {
        (bool extended, long fencingToken) = await connection.TryExtendLock(optsLock, optsOwner, optsExpires);

        if (format == "json")
            Console.WriteLine("{0}", extended);
        else
            Console.WriteLine("f{0} {1}", fencingToken, extended ? "extended" : "not extended");
    }
}
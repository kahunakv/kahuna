
using Kahuna.Client;

namespace Kahuna.Control.Commands;

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
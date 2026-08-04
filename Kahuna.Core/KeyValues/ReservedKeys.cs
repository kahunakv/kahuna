namespace Kahuna.Server.KeyValues;

/// <summary>
/// The <c>__kahuna:</c> key namespace is reserved for system-managed records (currently the durable
/// sequence records under <c>__kahuna:sequences:</c>). Public key-value entry points reject keys in
/// this namespace so a client cannot overwrite, delete, expire, or lock system state out from under
/// the subsystem that owns it; subsystems reach their own records through the internal
/// <c>System*</c> accessors on <see cref="KeyValuesManager"/>, which bypass the guard.
/// </summary>
internal static class ReservedKeys
{
    internal const string SystemPrefix = "__kahuna:";

    internal static bool IsReserved(string key)
    {
        return key.StartsWith(SystemPrefix, StringComparison.Ordinal);
    }
}

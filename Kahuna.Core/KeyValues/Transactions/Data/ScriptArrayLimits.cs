namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Bounds shared by every expression that materializes an array.
/// </summary>
internal static class ScriptArrayLimits
{
    /// <summary>
    /// Upper bound on the elements one expression may produce. The list is materialized, so without a bound a
    /// twenty-byte script asks for a multi-gigabyte allocation, and the transaction timeout cannot stop it
    /// because it is one uninterrupted statement. A hundred thousand elements is already far past any
    /// sensible loop inside a transaction.
    ///
    /// <para>One constant covers the range operator and the string split, because the danger is the same for
    /// both: the list itself, not where its elements came from. Two constants would let one of the two grow
    /// past the memory the other proved safe.</para>
    /// </summary>
    internal const long MaxElements = 100_000;
}

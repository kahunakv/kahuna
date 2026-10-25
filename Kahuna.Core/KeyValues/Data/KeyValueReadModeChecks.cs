
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The one shape rule of <see cref="KeyValueReadMode"/>: a statement read must carry a read timestamp. Without one
/// the read would silently become a latest read that pins, which is the shape the mode exists to avoid, so the
/// request is refused as malformed instead. Applied where a request enters (the routed read operations) and again
/// by the handler that serves it, so a request that reaches an actor by any other path is refused the same way.
/// </summary>
internal static class KeyValueReadModeChecks
{
    public static bool IsWellFormed(KeyValueReadMode readMode, HLCTimestamp readTimestamp) =>
        readMode == KeyValueReadMode.Default || !readTimestamp.IsNull();

    /// <summary>
    /// True when the read serves the transaction's own staged writes and nothing else of its MVCC state: a
    /// statement read with a timestamp and a transaction. A read of no transaction has no MVCC state, so the
    /// mode changes nothing for it.
    /// </summary>
    public static bool IsStatementRead(KeyValueReadMode readMode, HLCTimestamp transactionId, HLCTimestamp readTimestamp) =>
        readMode == KeyValueReadMode.StatementSnapshot && transactionId != HLCTimestamp.Zero && !readTimestamp.IsNull();
}

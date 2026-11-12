using System.Collections.Concurrent;

using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The interactive transactions whose coordinator session is open in this process. A key's actor asks it whether
/// the owner of a read pin is still running: a pin stays on its key after its transaction ends, until the actor
/// learns the outcome or the pin's lifetime passes, so the pin alone does not say.
///
/// <para>A transaction whose session is on another node is not known here, and is answered as not live. The
/// callers only use the answer to refuse earlier what would be refused anyway, so a wrong "not live" costs a later
/// refusal and never a wrong result.</para>
/// </summary>
internal static class LiveTransactions
{
    private static readonly ConcurrentDictionary<HLCTimestamp, byte> Open = new();

    internal static void Opened(HLCTimestamp transactionId) => Open.TryAdd(transactionId, 0);

    internal static void Closed(HLCTimestamp transactionId) => Open.TryRemove(transactionId, out _);

    internal static bool IsOpen(HLCTimestamp transactionId) => Open.ContainsKey(transactionId);
}

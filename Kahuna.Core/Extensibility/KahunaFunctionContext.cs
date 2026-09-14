
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

using Kommander.Time;

namespace Kahuna.Extensibility;

/// <summary>
/// What a user-defined function is told about the call it is serving.
///
/// <para>It carries identity and diagnostics only. It deliberately exposes no way to read or write
/// keys: the call happens in the middle of a transaction that holds locks and write intents, and a
/// re-entrant read or write can deadlock against the actor that is waiting for this call to return.
/// Read the data in the script and pass the value in as an argument instead.</para>
/// </summary>
public readonly struct KahunaFunctionContext
{
    /// <summary>The name the script used to call this function.</summary>
    public string FunctionName { get; }

    /// <summary>The 1-based script line of the call site. Used in error messages.</summary>
    public int Line { get; }

    /// <summary>
    /// The hybrid logical clock timestamp that identifies the transaction this call belongs to.
    /// It is <see cref="HLCTimestamp.Zero"/> for a single command that runs outside a transaction.
    /// </summary>
    public HLCTimestamp TransactionId { get; }

    /// <summary>
    /// The snapshot timestamp the transaction reads at, or <see cref="HLCTimestamp.Zero"/> when the
    /// transaction reads the latest value. Prefer it to the wall clock: it is the same on every
    /// attempt of one transaction, and it orders consistently across nodes.
    /// </summary>
    public HLCTimestamp ReadTimestamp { get; }

    /// <summary>The name of the node that evaluates the script.</summary>
    public string NodeName { get; }

    /// <summary>
    /// A logger for this call. Use it sparingly. This is the request path, and the transaction holds
    /// locks while the function runs.
    /// </summary>
    public ILogger Logger { get; }

    public KahunaFunctionContext(
        string functionName,
        int line,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string nodeName,
        ILogger logger
    )
    {
        FunctionName = functionName;
        Line = line;
        TransactionId = transactionId;
        ReadTimestamp = readTimestamp;
        NodeName = nodeName;
        Logger = logger;
    }

    /// <summary>
    /// Fails the call with a message. The transaction rolls back and the client sees <c>Errored</c>.
    /// </summary>
    [DoesNotReturn]
    public void Fail(string message)
    {
        throw new KahunaFunctionException(message);
    }
}

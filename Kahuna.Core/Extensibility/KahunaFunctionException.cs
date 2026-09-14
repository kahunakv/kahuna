
namespace Kahuna.Extensibility;

/// <summary>
/// Reports a deterministic, script-level failure from a user-defined function.
///
/// <para>Throw it from a function when the arguments are wrong, or when the function cannot produce
/// a result for the input it was given. The engine turns it into a script error, so the transaction
/// rolls back and the client sees <c>Errored</c> with the message below. <c>Errored</c> tells the
/// client the request is wrong and a retry of the same request will fail again.</para>
///
/// <para>Any other exception a function throws produces the same <c>Errored</c> outcome, but the
/// engine also logs it as unexpected. Use this type for a failure you meant to report.</para>
/// </summary>
public sealed class KahunaFunctionException : Exception
{
    public KahunaFunctionException()
    {
    }

    public KahunaFunctionException(string message) : base(message)
    {
    }

    public KahunaFunctionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

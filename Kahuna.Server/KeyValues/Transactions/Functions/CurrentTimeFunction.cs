
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to retrieve the current UTC time in Unix timestamp format
/// (milliseconds since Unix epoch).
/// </summary>
/// <remarks>
/// The function does not accept any arguments and will throw an exception if arguments are provided.
/// It returns a KeyValueExpressionResult containing the current UTC time as a Unix timestamp.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown when the function is called with an invalid number of arguments.
/// </exception>
internal static class CurrentTimeFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 0)
            throw new KahunaScriptException("Invalid number of arguments for 'current_time' function", ast.yyline);

        return new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }
}
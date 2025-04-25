
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides the implementation for the `is_long` function, used to determine
/// whether a given argument represents a value of type LongType.
/// </summary>
/// <remarks>
/// This function validates that a single argument is provided and verifies its type.
/// If the argument is of type Long, the function returns a positive result.
/// Otherwise, an exception is thrown or a negative result is returned.
/// </remarks>
internal static class IsLongFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'is_long' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return new(argument.Type == KeyValueExpressionType.LongType);
    }
}

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Represents a static utility class that provides the implementation for the 'is_double' function
/// used in evaluating whether a given argument is of a double type.
/// </summary>
/// <remarks>
/// The 'is_double' function is utilized within scripts to determine whether a provided argument
/// is of the DoubleType. The function ensures that exactly one argument is passed for validation.
/// </remarks>
internal static class IsDoubleFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'is_double' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return new(argument.Type == KeyValueExpressionType.DoubleType);
    }
}
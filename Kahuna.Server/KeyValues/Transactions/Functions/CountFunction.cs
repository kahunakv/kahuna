
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to execute the 'count' function within key-value transactions.
/// </summary>
/// <remarks>
/// This function executes operations to count the number of elements in an array-type argument.
/// </remarks>
internal static class CountFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'count' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.ArrayType => new(arg.ArrayValue?.Count ?? 0),
            _ => throw new KahunaScriptException($"Cannot use 'count' function on argument {arg.Type}", ast.yyline)
        };
    }
}
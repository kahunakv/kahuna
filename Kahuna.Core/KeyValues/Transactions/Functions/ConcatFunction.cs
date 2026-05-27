
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides a functionality to concatenate two string values during key-value transaction processing.
/// This static class encapsulates the implementation of a "concat" function, used to combine two
/// string arguments into a single resultant string.
/// </summary>
internal static class ConcatFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'concat' function", ast.yyline);

        KeyValueExpressionResult arg1 = arguments[0];
        KeyValueExpressionResult arg2 = arguments[0];

        if (arg1.Type != KeyValueExpressionType.StringType || arg2.Type != KeyValueExpressionType.StringType)
            throw new KahunaScriptException($"Cannot use 'concat' function on argument {arg1.Type} + {arg2.Type}", ast.yyline);

        return new(string.Concat(arg1.StrValue ?? "", arg2.StrValue ?? ""));
    }
}
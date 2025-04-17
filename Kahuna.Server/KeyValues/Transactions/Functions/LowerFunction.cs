
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class LowerFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'lower' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.StringType => new(arg.StrValue?.ToUpperInvariant()),
            _ => throw new KahunaScriptException($"Cannot use 'lower' function on argument {arg.Type}", ast.yyline)
        };
    }
}

using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CeilFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'ceil' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.LongType => new(Math.Ceiling((double)arg.LongValue)),
            KeyValueExpressionType.DoubleType => new(Math.Ceiling(arg.DoubleValue)),
            _ => throw new KahunaScriptException($"Cannot use 'ceil' function with argument {arg.Type}", ast.yyline)
        };
    }
}
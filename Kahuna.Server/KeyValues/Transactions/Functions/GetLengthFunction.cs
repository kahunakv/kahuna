
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class GetLengthFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'length' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.StringType => new() { Type = KeyValueExpressionType.LongType, LongValue = arg.StrValue?.Length ?? 0 },
            _ => throw new KahunaScriptException($"Cannot use 'length' function on argument {arg.Type}", ast.yyline)
        };
    }
}
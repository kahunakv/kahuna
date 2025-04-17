
using System.Text.Json;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class ToJsonFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_json' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.LongType => new(JsonSerializer.Serialize(arg.LongValue)),
            KeyValueExpressionType.DoubleType => new(JsonSerializer.Serialize(arg.DoubleValue)),
            KeyValueExpressionType.StringType => new(JsonSerializer.Serialize(arg.StrValue)),
            KeyValueExpressionType.ArrayType => new(JsonSerializer.Serialize(arg.ArrayValue)),
            _ => throw new KahunaScriptException($"Cannot use 'to_json' function on argument {arg.Type}", ast.yyline)
        };
    }
}
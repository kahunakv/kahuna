
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Functions;

internal static class CastToLongFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_int' function", ast.yyline);

        return arguments[0].Type switch
        {
            KeyValueExpressionType.Bool => new() { Type = KeyValueExpressionType.Long, LongValue = arguments[0].BoolValue ? 1 : 0 },
            KeyValueExpressionType.Long => new() { Type = KeyValueExpressionType.Long, LongValue = arguments[0].LongValue },
            KeyValueExpressionType.Double => new() { Type = KeyValueExpressionType.Long, LongValue = (long)arguments[0].DoubleValue },
            KeyValueExpressionType.String => new() { Type = KeyValueExpressionType.Long, LongValue = long.Parse(arguments[0].StrValue ?? "0") },
            _ => throw new KahunaScriptException($"Cannot cast {arguments[0].Type} to int", ast.yyline)
        };
    }
}
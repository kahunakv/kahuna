
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Functions;

internal static class CastToBoolFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_bool' function", ast.yyline);

        return arguments[0].Type switch
        {
            KeyValueExpressionType.Bool => new() { Type = KeyValueExpressionType.Bool, BoolValue = arguments[0].BoolValue },
            KeyValueExpressionType.Long => new() { Type = KeyValueExpressionType.Bool, BoolValue = arguments[0].LongValue != 0 },
            KeyValueExpressionType.Double => new() { Type = KeyValueExpressionType.Bool, BoolValue = arguments[0].DoubleValue != 0 },
            KeyValueExpressionType.String => new() { Type = KeyValueExpressionType.Bool, BoolValue = string.Compare(arguments[0].StrValue, "true", StringComparison.Ordinal) == 0 },
            _ => throw new KahunaScriptException($"Cannot cast {arguments[0].Type} to bool", ast.yyline)
        };
    }
}
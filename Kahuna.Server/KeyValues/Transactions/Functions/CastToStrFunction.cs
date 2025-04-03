
using System.Globalization;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Functions;

internal static class CastToStrFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_str' function", ast.yyline);

        return arguments[0].Type switch
        {
            KeyValueExpressionType.LongType => new() { Type = KeyValueExpressionType.StringType, StrValue = arguments[0].LongValue.ToString() },
            KeyValueExpressionType.DoubleType => new() { Type = KeyValueExpressionType.StringType, StrValue = arguments[0].DoubleValue.ToString(CultureInfo.InvariantCulture) },
            KeyValueExpressionType.StringType => new() { Type = KeyValueExpressionType.StringType, StrValue = arguments[0].StrValue ?? "" },
            KeyValueExpressionType.NullType => new() { Type = KeyValueExpressionType.StringType, StrValue = arguments[0].StrValue ?? "" },
            KeyValueExpressionType.BoolType => new() { Type = KeyValueExpressionType.StringType, StrValue = arguments[0].BoolValue.ToString() },
            _ => throw new KahunaScriptException($"Cannot cast {arguments[0].Type} to string", ast.yyline)
        };
    }
}
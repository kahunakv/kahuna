
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
            KeyValueExpressionType.Long => new() { Type = KeyValueExpressionType.String, StrValue = arguments[0].LongValue.ToString() },
            KeyValueExpressionType.Double => new() { Type = KeyValueExpressionType.String, StrValue = arguments[0].DoubleValue.ToString(CultureInfo.InvariantCulture) },
            KeyValueExpressionType.String => new() { Type = KeyValueExpressionType.String, StrValue = arguments[0].StrValue ?? "" },
            KeyValueExpressionType.Null => new() { Type = KeyValueExpressionType.String, StrValue = arguments[0].StrValue ?? "" },
            KeyValueExpressionType.Bool => new() { Type = KeyValueExpressionType.String, StrValue = arguments[0].BoolValue.ToString() },
            _ => throw new KahunaScriptException($"Cannot cast {arguments[0].Type} to string", ast.yyline)
        };
    }
}
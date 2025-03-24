
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Functions;

internal static class CastToLongFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_int' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return argument.Type switch
        {
            KeyValueExpressionType.Bool => new() { Type = KeyValueExpressionType.Long, LongValue = argument.BoolValue ? 1 : 0 },
            KeyValueExpressionType.Long => new() { Type = KeyValueExpressionType.Long, LongValue = argument.LongValue },
            KeyValueExpressionType.Double => new() { Type = KeyValueExpressionType.Long, LongValue = (long)argument.DoubleValue },
            KeyValueExpressionType.String => new() { Type = KeyValueExpressionType.Long, LongValue = TryCastString(ast, argument)  },
            _ => throw new KahunaScriptException($"Cannot cast {argument.Type} to int", ast.yyline)
        };
    }

    private static long TryCastString(NodeAst ast, KeyValueExpressionResult argument)
    {
        if (string.IsNullOrEmpty(argument.StrValue))
            return 0;
        
        if (long.TryParse(argument.StrValue, out long converted))
            return converted;

        throw new KahunaScriptException($"Cannot cast string value '{argument.StrValue}' to int", ast.yyline);
    }
}
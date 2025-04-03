
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CastToLongFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_int' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return argument.Type switch
        {
            KeyValueExpressionType.BoolType => new() { Type = KeyValueExpressionType.LongType, LongValue = argument.BoolValue ? 1 : 0 },
            KeyValueExpressionType.LongType => new() { Type = KeyValueExpressionType.LongType, LongValue = argument.LongValue },
            KeyValueExpressionType.DoubleType => new() { Type = KeyValueExpressionType.LongType, LongValue = (long)argument.DoubleValue },
            KeyValueExpressionType.StringType => new() { Type = KeyValueExpressionType.LongType, LongValue = TryCastString(ast, argument)  },
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

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CastToDoubleFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_double' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return argument.Type switch
        {
            KeyValueExpressionType.BoolType => new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = argument.BoolValue ? 1 : 0 },
            KeyValueExpressionType.DoubleType => new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = argument.DoubleValue },
            KeyValueExpressionType.LongType => new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = argument.LongValue },
            KeyValueExpressionType.StringType => new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = TryCastString(ast, argument)  },
            _ => throw new KahunaScriptException($"Cannot cast {argument.Type} to double", ast.yyline)
        };
    }

    private static double TryCastString(NodeAst ast, KeyValueExpressionResult argument)
    {
        if (string.IsNullOrEmpty(argument.StrValue))
            return 0;
        
        if (double.TryParse(argument.StrValue, out double converted))
            return converted;

        throw new KahunaScriptException($"Cannot cast string value '{argument.StrValue}' to double", ast.yyline);
    }
}
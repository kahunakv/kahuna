
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CastToBoolFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_bool' function", ast.yyline);

        return arguments[0].Type switch
        {
            KeyValueExpressionType.BoolType => arguments[0],
            KeyValueExpressionType.LongType => new(arguments[0].LongValue != 0),
            KeyValueExpressionType.DoubleType => new(arguments[0].DoubleValue != 0),
            KeyValueExpressionType.StringType => new(string.Compare(arguments[0].StrValue, "true", StringComparison.InvariantCultureIgnoreCase) == 0),
            _ => throw new KahunaScriptException($"Cannot cast {arguments[0].Type} to bool", ast.yyline)
        };
    }
}
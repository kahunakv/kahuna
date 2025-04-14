
using System.Globalization;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CastToStrFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_str' function", ast.yyline);

        return arguments[0].Type switch
        {
            KeyValueExpressionType.LongType => new(arguments[0].LongValue.ToString(), -1, 0),
            KeyValueExpressionType.DoubleType => new(arguments[0].DoubleValue.ToString(CultureInfo.InvariantCulture), -1, 0),
            KeyValueExpressionType.StringType => new(arguments[0].StrValue ?? "", -1, 0),
            KeyValueExpressionType.NullType => new(arguments[0].StrValue ?? "", -1, 0),
            KeyValueExpressionType.BoolType => new(arguments[0].BoolValue.ToString().ToLowerInvariant(), -1, 0),
            _ => throw new KahunaScriptException($"Cannot cast {arguments[0].Type} to string", ast.yyline)
        };
    }
}
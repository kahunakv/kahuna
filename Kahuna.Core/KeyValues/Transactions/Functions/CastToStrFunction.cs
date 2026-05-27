
using System.Globalization;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides the implementation for the `to_str` function, which converts a given argument to its string representation.
/// </summary>
/// <remarks>
/// This function processes various data types including Long, Double, String, Null, and Boolean, and returns their string equivalents.
/// Unsupported types will result in an exception.
/// </remarks>
/// <exception cref="Kahuna.Server.ScriptParser.KahunaScriptException">
/// Thrown when the number of arguments is invalid or when the argument type cannot be converted to a string.
/// </exception>
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
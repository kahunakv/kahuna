
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to cast a given argument to a boolean value.
/// </summary>
/// <remarks>
/// This class implements the `to_bool` function, which converts a single argument
/// to boolean based on its type. Supported conversions include:
/// - Boolean: Returns the same boolean value.
/// - Long: Converts non-zero values to `true`, zero to `false`.
/// - Double: Converts non-zero values to `true`, zero to `false`.
/// - String: Compares the string (case-insensitively) with "true" to determine the boolean value.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown if the number of arguments provided is not exactly 1,
/// or if the argument type is unsupported for boolean conversion.
/// </exception>
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
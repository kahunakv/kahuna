using System.Globalization;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides a static method to cast a variety of input types to a <c>long</c> type.
/// Supports calling through various aliases such as "to_int", "to_integer", "to_long", and "to_number".
/// </summary>
/// <remarks>
/// The function processes a single argument and attempts to cast it into a <c>long</c>, based on its type:
/// - <c>BoolType</c>: Converts <c>true</c> to <c>1</c> and <c>false</c> to <c>0</c>.
/// - <c>LongType</c>: Returns the argument as-is.
/// - <c>DoubleType</c>: Truncates toward zero, so 2.5 gives 2 and -1.5 gives -1. A value outside the long
///   range is a script error: a plain cast would saturate it to long.MinValue or long.MaxValue.
/// - <c>StringType</c>: Parses the string as a whole number with the invariant culture, so the result does
///   not depend on the culture of the server. An empty string gives <c>0</c>.
/// Throws a <c>KahunaScriptException</c> for unsupported types, for a value it cannot convert, or if the
/// number of arguments is incorrect.
/// </remarks>
internal static class CastToLongFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_int' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return argument.Type switch
        {
            KeyValueExpressionType.BoolType => new(argument.BoolValue ? 1 : 0),
            KeyValueExpressionType.LongType => argument,
            KeyValueExpressionType.DoubleType => new(TruncateDouble(ast, argument.DoubleValue)),
            KeyValueExpressionType.StringType => new(TryCastString(ast, argument)),
            _ => throw new KahunaScriptException($"Cannot cast {argument.Type} to int", ast.yyline)
        };
    }

    private static long TruncateDouble(NodeAst ast, double value)
    {
        if (ScriptNumeric.TryTruncateToLong(value, out long truncated))
            return truncated;

        throw new KahunaScriptException($"Cannot cast {value.ToString(CultureInfo.InvariantCulture)} to int: out of range", ast.yyline);
    }

    private static long TryCastString(NodeAst ast, KeyValueExpressionResult argument)
    {
        if (string.IsNullOrEmpty(argument.StrValue))
            return 0;
        
        if (long.TryParse(argument.StrValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out long converted))
            return converted;

        throw new KahunaScriptException($"Cannot cast string value '{argument.StrValue}' to int", ast.yyline);
    }
}

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
/// - <c>DoubleType</c>: Performs a cast by truncating the fractional part.
/// - <c>StringType</c>: Attempts to parse the string into a <c>long</c>, throwing an exception if parsing fails.
/// Throws a <c>KahunaScriptException</c> for unsupported types or if the number of arguments is incorrect.
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
            KeyValueExpressionType.DoubleType => new((long)argument.DoubleValue),
            KeyValueExpressionType.StringType => new(TryCastString(ast, argument)),
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
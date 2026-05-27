
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to cast a single argument to a double value.
/// </summary>
/// <remarks>
/// This function is designed to handle various input types and convert them to a double representation.
/// Acceptable argument types include Boolean, numeric (long or double), and string.
/// If the argument is of type Boolean, the result is 1.0 for true and 0.0 for false.
/// If the argument is already of type Double, it is returned as is.
/// Long type arguments are cast to equivalent double values.
/// String arguments are parsed and converted to double.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown when the number of arguments is not exactly one,
/// or if the argument type cannot be cast to a double.
/// </exception>
internal static class CastToDoubleFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'to_double' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return argument.Type switch
        {
            KeyValueExpressionType.BoolType => new((double)(argument.BoolValue ? 1 : 0)),
            KeyValueExpressionType.DoubleType => argument,
            KeyValueExpressionType.LongType => new((double)argument.LongValue),
            KeyValueExpressionType.StringType => new(TryCastString(ast, argument)),
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
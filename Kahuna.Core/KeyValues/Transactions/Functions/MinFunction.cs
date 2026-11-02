
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides an implementation of the 'min' function that evaluates the minimum
/// between two numeric arguments of type long or double at runtime.
/// </summary>
/// <remarks>
/// The 'min' function identifies the smaller value between two numeric arguments.
/// Both arguments must be either a long or a double type. If either argument is
/// of a non-numeric type, the method throws an exception.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown if the number of arguments is not exactly two, or if any of the arguments
/// are not of the expected numeric types (long or double).
/// </exception>
internal static class MinFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'min' function", ast.yyline);

        KeyValueExpressionResult arg1 = arguments[0];
        KeyValueExpressionResult arg2 = arguments[1];

        if (arg1.Type != KeyValueExpressionType.LongType && arg1.Type != KeyValueExpressionType.DoubleType)
            throw new KahunaScriptException($"Cannot use 'min' function with argument {arg1.Type}", ast.yyline);

        if (arg2.Type != KeyValueExpressionType.LongType && arg2.Type != KeyValueExpressionType.DoubleType)
            throw new KahunaScriptException($"Cannot use 'min' function with argument {arg2.Type}", ast.yyline);
                       
        // The arguments compare by their exact values, and the chosen argument keeps its own type: Math.Min
        // of a long and a double converted the long to a double, which lost the low bits of a long above
        // 2^53. When the two are equal, the first argument is returned.
        ScriptNumber first = ToNumber(arg1);
        ScriptNumber second = ToNumber(arg2);

        return ScriptNumeric.Compare(first, second) <= 0 ? first.ToResult() : second.ToResult();
    }

    private static ScriptNumber ToNumber(KeyValueExpressionResult argument) =>
        argument.Type == KeyValueExpressionType.LongType ? new(argument.LongValue) : new(argument.DoubleValue);
}

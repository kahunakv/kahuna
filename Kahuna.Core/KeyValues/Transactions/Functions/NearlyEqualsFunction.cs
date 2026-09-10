
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Compares two numbers within a tolerance the caller states: 'nearly_equals(a, b, tolerance)' is true when
/// the two differ by no more than the tolerance. The '==' operator is exact, so this is how a script asks
/// for an approximate comparison of measured or computed values.
/// </summary>
internal static class NearlyEqualsFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 3)
            throw new KahunaScriptException("Invalid number of arguments for 'nearly_equals' function", ast.yyline);

        double left = ToDouble(ast, arguments[0]);
        double right = ToDouble(ast, arguments[1]);
        double tolerance = ToDouble(ast, arguments[2]);

        if (double.IsNaN(tolerance) || tolerance < 0)
            throw new KahunaScriptException("Tolerance for 'nearly_equals' function must not be negative", ast.yyline);

        return new(Math.Abs(left - right) <= tolerance);
    }

    private static double ToDouble(NodeAst ast, KeyValueExpressionResult argument)
    {
        return argument.Type switch
        {
            KeyValueExpressionType.LongType => argument.LongValue,
            KeyValueExpressionType.DoubleType => argument.DoubleValue,
            _ => throw new KahunaScriptException($"Cannot use 'nearly_equals' function with argument {argument.Type}", ast.yyline)
        };
    }
}

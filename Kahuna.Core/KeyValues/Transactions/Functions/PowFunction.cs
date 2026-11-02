
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to execute a power function ('pow') operation within the context of key-value expressions.
/// </summary>
internal static class PowFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'pow' function", ast.yyline);

        KeyValueExpressionResult arg1 = arguments[0];
        KeyValueExpressionResult arg2 = arguments[1];

        if (arg1.Type != KeyValueExpressionType.LongType && arg1.Type != KeyValueExpressionType.DoubleType)
            throw new KahunaScriptException($"Cannot use 'pow' function with argument {arg1.Type}", ast.yyline);

        if (arg2.Type != KeyValueExpressionType.LongType && arg2.Type != KeyValueExpressionType.DoubleType)
            throw new KahunaScriptException($"Cannot use 'pow' function with argument {arg2.Type}", ast.yyline);
                       
        // Two finite arguments can still give a value that is not finite: pow(10, 400) overflows, pow(0, -1)
        // divides by zero, and pow(-1, 0.5) has no real result. None of them is a script value.
        double result = Math.Pow(ToDouble(arg1), ToDouble(arg2));

        return new(ScriptNumeric.RequireFiniteResult(result, "pow", ast));
    }

    private static double ToDouble(KeyValueExpressionResult argument) =>
        argument.Type == KeyValueExpressionType.LongType ? argument.LongValue : argument.DoubleValue;
}

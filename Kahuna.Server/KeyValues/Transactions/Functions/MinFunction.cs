
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
                       
        return arg1.Type switch
        {
            KeyValueExpressionType.LongType when arg2.Type == KeyValueExpressionType.LongType => 
                new(Math.Min(arg1.LongValue, arg2.LongValue)),
            
            KeyValueExpressionType.LongType when arg2.Type == KeyValueExpressionType.DoubleType => 
                new(Math.Min(arg1.LongValue, arg2.DoubleValue)),
            
            KeyValueExpressionType.DoubleType when arg2.Type == KeyValueExpressionType.LongType => 
                new(Math.Min(arg1.DoubleValue, arg2.LongValue)),
            
            KeyValueExpressionType.DoubleType when arg2.Type == KeyValueExpressionType.DoubleType => 
                new(Math.Min(arg1.DoubleValue, arg2.DoubleValue)),          
            
            _ => 
                throw new KahunaScriptException($"Cannot use 'min' function with argument {arg1.Type}", ast.yyline)
        };
    }
}
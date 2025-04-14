
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class MaxFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'max' function", ast.yyline);

        KeyValueExpressionResult arg1 = arguments[0];
        KeyValueExpressionResult arg2 = arguments[1];

        if (arg1.Type != KeyValueExpressionType.LongType && arg1.Type != KeyValueExpressionType.DoubleType)
            throw new KahunaScriptException($"Cannot use 'max' function with argument {arg1.Type}", ast.yyline);

        if (arg2.Type != KeyValueExpressionType.LongType && arg2.Type != KeyValueExpressionType.DoubleType)
            throw new KahunaScriptException($"Cannot use 'max' function with argument {arg2.Type}", ast.yyline);
                       
        return arg1.Type switch
        {
            KeyValueExpressionType.LongType when arg2.Type == KeyValueExpressionType.LongType => 
                new(Math.Max(arg1.LongValue, arg2.LongValue)),
            
            KeyValueExpressionType.LongType when arg2.Type == KeyValueExpressionType.DoubleType => 
                new(Math.Max(arg1.LongValue, arg2.DoubleValue)),
            
            KeyValueExpressionType.DoubleType when arg2.Type == KeyValueExpressionType.LongType => 
                new(Math.Max(arg1.DoubleValue, arg2.LongValue)),
            
            KeyValueExpressionType.DoubleType when arg2.Type == KeyValueExpressionType.DoubleType => 
                new(Math.Max(arg1.DoubleValue, arg2.DoubleValue)),          
            
            _ => 
                throw new KahunaScriptException($"Cannot use 'max' function with argument {arg1.Type}", ast.yyline)
        };
    }
}
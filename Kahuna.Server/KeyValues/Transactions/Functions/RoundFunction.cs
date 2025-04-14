
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class RoundFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'round' function", ast.yyline);

        KeyValueExpressionResult arg1 = arguments[0];
        KeyValueExpressionResult arg2 = arguments[1];

        if (arg1.Type != KeyValueExpressionType.LongType && arg1.Type != KeyValueExpressionType.DoubleType)
            throw new KahunaScriptException($"Cannot use 'round' function with argument {arg1.Type}", ast.yyline);

        if (arg2.Type != KeyValueExpressionType.LongType)
            throw new KahunaScriptException($"Cannot use 'round' function with argument {arg2.Type}", ast.yyline);
        
        if (arg2.LongValue >= int.MaxValue)
            throw new KahunaScriptException($"Invalid value for digits {arg2.LongValue} in 'round' function", ast.yyline);
                       
        return arg1.Type switch
        {
            KeyValueExpressionType.LongType when arg2.Type == KeyValueExpressionType.LongType => 
                new(Math.Round((double)arg1.LongValue, (int)arg2.LongValue)),
                        
            KeyValueExpressionType.DoubleType when arg2.Type == KeyValueExpressionType.LongType => 
                new(Math.Round(arg1.DoubleValue, (int)arg2.LongValue)),
                        
            _ => 
                throw new KahunaScriptException($"Cannot use 'round' function with argument {arg1.Type}", ast.yyline)
        };
    }
}
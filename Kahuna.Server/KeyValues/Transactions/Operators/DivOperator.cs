
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class DivOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.LongType, LongValue = left.LongValue / right.LongValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue / right.LongValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.LongValue / right.DoubleValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue / right.DoubleValue };
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " / " + right.Type, ast.yyline);
        }
    }
}
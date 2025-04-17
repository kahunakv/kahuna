
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal sealed class RangeOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        if (left.Type != KeyValueExpressionType.LongType || right.Type != KeyValueExpressionType.LongType)
            throw new KahunaScriptException("Invalid operands for range operator: " + left.Type + " - " + right.Type, ast.yyline);
        
        List<KeyValueExpressionResult> result = [];
        
        foreach (int valueRange in Enumerable.Range((int)left.LongValue, (int)right.LongValue))
            result.Add(new(valueRange));
        
        return new(result);
    }
}
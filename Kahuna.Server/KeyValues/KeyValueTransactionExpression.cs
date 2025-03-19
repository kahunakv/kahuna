
using System.Text;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues;

public static class KeyValueTransactionExpression
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        switch (ast.nodeType)
        {
            case NodeType.Identifier:
                return context.GetVariable(ast.yytext!).ToExpressionResult();
            
            case NodeType.Integer:
                return new() { Type = KeyValueExpressionType.Long, LongValue = long.Parse(ast.yytext!) };
            
            case NodeType.String:
                return new() { Type = KeyValueExpressionType.String, StrValue = ast.yytext!.Trim('\"') };
            
            case NodeType.Float:
                return new() { Type = KeyValueExpressionType.String, DoubleValue = double.Parse(ast.yytext!) };
            
            case NodeType.Boolean:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = ast.yytext! == "true" };
            
            case NodeType.Equals:
                return EvalEquals(context, ast);
            
            case NodeType.NotEquals:
            {
                KeyValueExpressionResult result = EvalEquals(context, ast);
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = !result.BoolValue };
            }
            
            case NodeType.Add:
                return EvalAdd(context, ast);
            
            case NodeType.Subtract:
                return EvalSub(context, ast);
            
            case NodeType.Mult:
                return EvalMult(context, ast);
            
            case NodeType.Div:
                return EvalDiv(context, ast);
            
            case NodeType.LessThan:
            case NodeType.GreaterThan:
            case NodeType.LessThanEquals:
            case NodeType.GreaterThanEquals:
            case NodeType.And:
            case NodeType.Or:
            case NodeType.Not:
            case NodeType.FuncCall:
                throw new NotImplementedException();
            
            case NodeType.StmtList:
            case NodeType.Set:
            case NodeType.Get:
            case NodeType.Eset:
            case NodeType.Eget:
            case NodeType.If:
            case NodeType.SetNotExists:
            case NodeType.SetExists:
            case NodeType.Begin:
            case NodeType.Rollback:
            case NodeType.Commit:
            case NodeType.Return:
            case NodeType.ArgumentList:
            case NodeType.Delete:
            case NodeType.Edelete:
                break;
            
            default:
                throw new NotImplementedException();
        }

        return new() { Type = KeyValueExpressionType.Null };
    }

    private static KeyValueExpressionResult EvalEquals(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid left expression");
                
        if (ast.rightAst is null)
            throw new Exception("Invalid right expression");
        
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);

        switch (left.Type)
        {
            case KeyValueExpressionType.Null when right.Type == KeyValueExpressionType.Null:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = true };
            
            case KeyValueExpressionType.Null when right.Type != KeyValueExpressionType.Null:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = false };
            
            case KeyValueExpressionType.Bool when right.Type == KeyValueExpressionType.Bool:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = left.BoolValue == right.BoolValue };
            
            case KeyValueExpressionType.String when right.Type == KeyValueExpressionType.String:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = left.StrValue == right.StrValue };
            
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = left.LongValue == right.LongValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = Math.Abs(left.DoubleValue - right.DoubleValue) < 0.01 };
            
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = Math.Abs(left.LongValue - right.DoubleValue) < 0.01 };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = Math.Abs(left.DoubleValue - right.LongValue) < 0.01 };
            
            case KeyValueExpressionType.Bytes when right.Type == KeyValueExpressionType.String:
                byte[] rightBytes = right.StrValue is not null ? Encoding.UTF8.GetBytes(right.StrValue) : [];
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(rightBytes) };
            
            case KeyValueExpressionType.String when right.Type == KeyValueExpressionType.Bytes:
                byte[] leftBytes = left.StrValue is not null ? Encoding.UTF8.GetBytes(left.StrValue) : [];
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = ((ReadOnlySpan<byte>)right.BytesValue).SequenceEqual(leftBytes) };
            
            case KeyValueExpressionType.Bytes when right.Type == KeyValueExpressionType.Bytes:
                return new() { Type = KeyValueExpressionType.Bool, BoolValue = ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(right.BytesValue) };

            default:
                throw new Exception("Invalid operands: " + left.Type + " == " + right.Type);
        }
    }

    private static KeyValueExpressionResult EvalAdd(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid left expression");
                
        if (ast.rightAst is null)
            throw new Exception("Invalid right expression");
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Long, LongValue = left.LongValue + right.LongValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue + right.LongValue };
            
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.LongValue + right.DoubleValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue + right.DoubleValue };
                
            default:
                throw new Exception("Invalid operands: " + left.Type + " == " + right.Type);
        }
    }
    
    private static KeyValueExpressionResult EvalSub(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid left expression");
                
        if (ast.rightAst is null)
            throw new Exception("Invalid right expression");
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Long, LongValue = left.LongValue - right.LongValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue - right.LongValue };
            
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.LongValue - right.DoubleValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue - right.DoubleValue };
                
            default:
                throw new Exception("Invalid operands: " + left.Type + " == " + right.Type);
        }
    }
    
    private static KeyValueExpressionResult EvalMult(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid left expression");
                
        if (ast.rightAst is null)
            throw new Exception("Invalid right expression");
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Long, LongValue = left.LongValue * right.LongValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue * right.LongValue };
            
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.LongValue * right.DoubleValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue * right.DoubleValue };
                
            default:
                throw new Exception("Invalid operands: " + left.Type + " == " + right.Type);
        }
    }
    
    private static KeyValueExpressionResult EvalDiv(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new Exception("Invalid left expression");
                
        if (ast.rightAst is null)
            throw new Exception("Invalid right expression");
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Long, LongValue = left.LongValue / right.LongValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Long:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue / right.LongValue };
            
            case KeyValueExpressionType.Long when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.LongValue / right.DoubleValue };
            
            case KeyValueExpressionType.Double when right.Type == KeyValueExpressionType.Double:
                return new() { Type = KeyValueExpressionType.Double, DoubleValue = left.DoubleValue / right.DoubleValue };
                
            default:
                throw new Exception("Invalid operands: " + left.Type + " == " + right.Type);
        }
    }
}
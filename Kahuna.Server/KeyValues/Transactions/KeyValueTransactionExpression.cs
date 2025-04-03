
using System.Text;
using Kahuna.Server.KeyValues.Functions;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues;

public static class KeyValueTransactionExpression
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        switch (ast.nodeType)
        {
            case NodeType.Identifier:
                return context.GetVariable(ast, ast.yytext!);
            
            case NodeType.IntegerType:
                return new() { Type = KeyValueExpressionType.LongType, LongValue = long.Parse(ast.yytext!) };
            
            case NodeType.StringType:
                return new() { Type = KeyValueExpressionType.StringType, StrValue = ast.yytext! };
            
            case NodeType.FloatType:
                return new() { Type = KeyValueExpressionType.StringType, DoubleValue = double.Parse(ast.yytext!) };
            
            case NodeType.BooleanType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ast.yytext! == "true" };
            
            case NodeType.Placeholder:
                return new() { Type = KeyValueExpressionType.StringType, StrValue = "some value" };
            
            case NodeType.Equals:
                return EvalEquals(context, ast, "==");
            
            case NodeType.GreaterThan:
                return EvalGreaterThan(context, ast, ">");
            
            case NodeType.LessThan:
                return EvalLessThan(context, ast, "<");
            
            case NodeType.LessThanEquals:
            {
                KeyValueExpressionResult result = EvalGreaterThan(context, ast, "<=");
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = !result.BoolValue };
            }
            
            case NodeType.GreaterThanEquals:
            {
                KeyValueExpressionResult result = EvalLessThan(context, ast, ">=");
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = !result.BoolValue };
            }
            
            case NodeType.NotEquals:
            {
                KeyValueExpressionResult result = EvalEquals(context, ast, "!=");
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = !result.BoolValue };
            }
            
            case NodeType.Add:
                return EvalAdd(context, ast);
            
            case NodeType.Subtract:
                return EvalSub(context, ast);
            
            case NodeType.Mult:
                return EvalMult(context, ast);
            
            case NodeType.Div:
                return EvalDiv(context, ast);
            
            case NodeType.FuncCall:
                return EvalFuncCall(context, ast);
            
            case NodeType.And:
                return EvalAnd(context, ast);
            
            case NodeType.Or:
                return EvalOr(context, ast);
            
            case NodeType.Not:
                return EvalNot(context, ast);
            
            case NodeType.StmtList:
            case NodeType.Set:
            case NodeType.Get:
            case NodeType.Eset:
            case NodeType.Eget:
            case NodeType.If:
            case NodeType.SetNotExists:
            case NodeType.SetExists:
            case NodeType.SetCmp:
            case NodeType.SetCmpRev:
            case NodeType.Begin:
            case NodeType.Rollback:
            case NodeType.Commit:
            case NodeType.Return:
            case NodeType.ArgumentList:
            case NodeType.Delete:
            case NodeType.Edelete:
            case NodeType.Exists:
            case NodeType.Eexists:
                break;

            case NodeType.Let:
            case NodeType.Extend:
            case NodeType.Eextend:
            case NodeType.BeginOptionList:
            case NodeType.BeginOption:
            case NodeType.Sleep:
            default:
                throw new NotImplementedException();
        }

        return new() { Type = KeyValueExpressionType.NullType };
    }

    private static KeyValueExpressionResult EvalFuncCall(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
        
        List<KeyValueExpressionResult> arguments = [];
        
        GetFuncCallArguments(context, ast.rightAst, arguments);

        return ast.leftAst.yytext! switch
        {
            "to_int" or "to_long" or "to_number" => CastToLongFunction.Execute(ast, arguments),
            "to_str" => CastToStrFunction.Execute(ast, arguments),
            "to_bool" or "to_boolean" => CastToBoolFunction.Execute(ast, arguments),
            "revision" or "rev" => GetRevisionFunction.Execute(ast, arguments),
            "len" or "length" => GetLengthFunction.Execute(ast, arguments),
            _ => throw new KahunaScriptException($"Undefined function {ast.leftAst.yytext!} expression", ast.yyline)
        };
    }

    private static void GetFuncCallArguments(KeyValueTransactionContext context, NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.ArgumentList:
                {
                    if (ast.leftAst is not null)
                        GetFuncCallArguments(context, ast.leftAst, arguments);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                default:
                    arguments.Add(Eval(context, ast));
                    break;
            }

            break;
        }
    }

    private static KeyValueExpressionResult EvalEquals(KeyValueTransactionContext context, NodeAst ast, string operatorType)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
        
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);

        switch (left.Type)
        {
            case KeyValueExpressionType.NullType when right.Type == KeyValueExpressionType.NullType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = true };
            
            case KeyValueExpressionType.NullType when right.Type != KeyValueExpressionType.NullType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = false };
            
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.BoolValue == right.BoolValue };
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.StringType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.StrValue == right.StrValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue == right.LongValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = Math.Abs(left.DoubleValue - right.DoubleValue) < 0.01 };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = Math.Abs(left.LongValue - right.DoubleValue) < 0.01 };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = Math.Abs(left.DoubleValue - right.LongValue) < 0.01 };
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.StringType:
                byte[] rightBytes = right.StrValue is not null ? Encoding.UTF8.GetBytes(right.StrValue) : [];
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(rightBytes) };
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.BytesType:
                byte[] leftBytes = left.StrValue is not null ? Encoding.UTF8.GetBytes(left.StrValue) : [];
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ((ReadOnlySpan<byte>)right.BytesValue).SequenceEqual(leftBytes) };
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.BytesType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(right.BytesValue) };

            default:
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
        }
    }

    private static KeyValueExpressionResult EvalGreaterThan(KeyValueTransactionContext context, NodeAst ast, string operatorType)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);

        switch (left.Type)
        {
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue > right.LongValue };

            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue > right.LongValue };

            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue > right.DoubleValue };

            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue > right.DoubleValue };

            default:
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
        }
    }

    private static KeyValueExpressionResult EvalLessThan(KeyValueTransactionContext context, NodeAst ast, string operatorType)
        {
            if (ast.leftAst is null)
                throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
            if (ast.rightAst is null)
                throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
            KeyValueExpressionResult left = Eval(context, ast.leftAst);
            KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
            switch (left.Type)
            {
                case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                    return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue < right.LongValue };
            
                case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                    return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue < right.LongValue };
            
                case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                    return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue < right.DoubleValue };
            
                case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                    return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue < right.DoubleValue };
                
                default:
                    throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
            }
    }

    private static KeyValueExpressionResult EvalAdd(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.LongType, LongValue = left.LongValue + right.LongValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue + right.LongValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.LongValue + right.DoubleValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue + right.DoubleValue };
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " + " + right.Type, ast.yyline);
        }
    }
    
    private static KeyValueExpressionResult EvalSub(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.LongType, LongValue = left.LongValue - right.LongValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue - right.LongValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.LongValue - right.DoubleValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue - right.DoubleValue };
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " - " + right.Type, ast.yyline);
        }
    }
    
    private static KeyValueExpressionResult EvalMult(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.LongType, LongValue = left.LongValue * right.LongValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue * right.LongValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.LongValue * right.DoubleValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue * right.DoubleValue };
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " * " + right.Type, ast.yyline);
        }
    }
    
    private static KeyValueExpressionResult EvalDiv(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
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
    
    private static KeyValueExpressionResult EvalAnd(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.BoolValue && right.BoolValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue != 0 && right.LongValue != 0 };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue != 0 && right.LongValue != 0 };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue != 0 && right.DoubleValue != 0 };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue != 0 && right.DoubleValue != 0 };
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " and " + right.Type, ast.yyline);
        }
    }
    
    private static KeyValueExpressionResult EvalOr(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        KeyValueExpressionResult right = Eval(context, ast.rightAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.BoolValue || right.BoolValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue != 0 || right.LongValue != 0 };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue != 0 || right.LongValue != 0 };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue != 0 || right.DoubleValue != 0 };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue != 0 || right.DoubleValue != 0 };
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " or " + right.Type, ast.yyline);
        }
    }
    
    private static KeyValueExpressionResult EvalNot(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        KeyValueExpressionResult left = Eval(context, ast.leftAst);
        
        switch (left.Type)
        {
            case KeyValueExpressionType.BoolType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.BoolValue };
            
            case KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.LongValue != 0 };
            
            case KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.BoolType, BoolValue = left.DoubleValue != 0 };
                
            default:
                throw new KahunaScriptException("Invalid operands: not(" + left.Type + ")", ast.yyline);
        }
    }
}
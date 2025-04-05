
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CallFunction
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid function expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid function arguments expression", ast.yyline);
        
        if (string.IsNullOrEmpty(ast.leftAst.yytext))
            throw new KahunaScriptException("Invalid function name", ast.yyline);
        
        List<KeyValueExpressionResult> arguments = [];
        
        GetFuncCallArguments(context, ast.rightAst, arguments);

        return ast.leftAst.yytext switch
        {
            "to_int" or "to_integer" or "to_long" or "to_number" => CastToLongFunction.Execute(ast, arguments),
            "is_int" or "is_integer" or "is_long" => IsLongFunction.Execute(ast, arguments),
            "to_float" or "to_double" => CastToDoubleFunction.Execute(ast, arguments),
            "is_float" or "is_double" => IsDoubleFunction.Execute(ast, arguments),
            "to_str" or "to_string" => CastToStrFunction.Execute(ast, arguments),
            "is_str" or "is_string" => IsStringFunction.Execute(ast, arguments),
            "to_bool" or "to_boolean" => CastToBoolFunction.Execute(ast, arguments),
            "is_bool" or "is_boolean" => IsBoolFunction.Execute(ast, arguments),
            "is_null" => IsNullFunction.Execute(ast, arguments),
            "revision" or "rev" => GetRevisionFunction.Execute(ast, arguments),
            "expires" => GetExpiresFunction.Execute(ast, arguments),
            "len" or "length" => GetLengthFunction.Execute(ast, arguments),
            _ => throw new KahunaScriptException($"Undefined function {ast.leftAst.yytext} expression", ast.yyline)
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
                    arguments.Add(KeyValueTransactionExpression.Eval(context, ast));
                    break;
            }

            break;
        }
    }
}
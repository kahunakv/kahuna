
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CallFunction
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
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
}
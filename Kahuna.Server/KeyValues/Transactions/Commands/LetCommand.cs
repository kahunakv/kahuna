
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class LetCommand : BaseCommand
{
    public static void Execute(
        KeyValueTransactionContext context,
        NodeAst ast
    )
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid LET expression", ast.yyline);
        
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid LET expression", ast.yyline);
        
        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        context.Result = result.ToTransactionResult();
        
        context.SetVariable(ast.leftAst, ast.leftAst.yytext!, result);
    }
}
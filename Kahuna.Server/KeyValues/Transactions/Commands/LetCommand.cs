
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class LetCommand : BaseCommand
{
    public static KeyValueTransactionResult Execute(
        KeyValueTransactionContext context,
        NodeAst ast
    )
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid LET expression", ast.yyline);
        
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid LET expression", ast.yyline);
        
        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        context.SetVariable(ast.leftAst, ast.leftAst.yytext!, result);        
        
        return new()
        {
            ServedFrom = "",
            Type = KeyValueResponseType.Get,
            Values = [
                new()
                {
                    Key = ast.leftAst.yytext!,
                    Revision = result.Revision,
                    Expires = new(result.Expires, 0),
                    LastModified = HLCTimestamp.Zero
                }
            ]
        };
    }
}
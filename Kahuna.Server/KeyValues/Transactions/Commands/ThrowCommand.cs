
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class ThrowCommand : BaseCommand
{
    public static void Execute(KeyValueTransactionContext context, NodeAst ast, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid THROW expression", ast.yyline);
                    
        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        
        throw new KahunaScriptException(result.ToString(), ast.yyline);
    }
}
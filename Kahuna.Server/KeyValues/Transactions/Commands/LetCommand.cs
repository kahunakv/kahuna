
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command for assigning variables within a transaction context.
/// The LetCommand assigns the result of a evaluated right-hand expression to a specified
/// left-hand variable within the transaction context.
/// </summary>
internal sealed class LetCommand : BaseCommand
{
    public static KeyValueTransactionResult Execute(
        KeyValueTransactionContext context,
        NodeAst ast
    )
    {
        if (ast.leftAst is null || ast.rightAst is null)
            throw new KahunaScriptException("Invalid LET expression", ast.yyline);

        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        context.SetVariable(ast.leftAst, ast.leftAst.yytext!, result);

        return result.ToTransactionResult();
    }
}
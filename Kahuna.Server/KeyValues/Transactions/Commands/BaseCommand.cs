
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal abstract class BaseCommand
{
    protected static string GetKeyName(KeyValueTransactionContext context, NodeAst ast)
    {
        if (string.IsNullOrEmpty(ast.yytext))
            throw new KahunaScriptException($"Invalid key name type {ast.nodeType}", ast.yyline);
        
        return ast.nodeType switch
        {
            NodeType.Identifier => ast.yytext,
            NodeType.StringType => ast.yytext,
            NodeType.Placeholder => context.GetParameter(ast),
            _ => throw new KahunaScriptException($"Invalid key name type {ast.nodeType}", ast.yyline)
        };
    }
}
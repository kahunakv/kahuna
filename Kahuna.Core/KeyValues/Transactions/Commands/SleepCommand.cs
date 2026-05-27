
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class SleepCommand : BaseCommand
{
    public static async Task Execute(NodeAst ast, CancellationToken cancellationToken)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid SLEEP duration", ast.yyline);
                    
        if (!long.TryParse(ast.leftAst.yytext, out long duration))
            throw new KahunaScriptException($"Invalid SLEEP duration {duration}", ast.yyline);
                    
        if (duration is < 0 or > 300000)
            throw new KahunaScriptException($"Invalid SLEEP duration {duration}", ast.yyline);
                    
        await Task.Delay(TimeSpan.FromMilliseconds(duration), cancellationToken);
    }
}
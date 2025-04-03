
namespace Kahuna.Server.ScriptParser;

public sealed class ScriptCacheEntry
{
    public string Hash { get; }
    
    public NodeAst Ast { get; }
    
    public DateTime Expiration { get; set; }

    public ScriptCacheEntry(string hash, NodeAst ast, DateTime expiration)
    {
        Hash = hash;
        Ast = ast;
        Expiration = expiration;
    }
}

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents a cache entry for a script. Each entry contains the script's hash,
/// its abstract syntax tree (AST) and an expiration date and time.
/// </summary>
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

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents a cache entry for a script. Each entry contains the script's hash,
/// its abstract syntax tree (AST) and an expiration timestamp (Environment.TickCount64, milliseconds).
/// </summary>
public sealed class ScriptCacheEntry
{
    public string Hash { get; }

    public NodeAst Ast { get; }

    /// <summary>
    /// Absolute expiry expressed as <see cref="Environment.TickCount64"/> milliseconds.
    /// Using a monotonic source avoids clock-adjustment surprises.
    /// </summary>
    public long ExpiresAt { get; set; }

    public ScriptCacheEntry(string hash, NodeAst ast, long expiresAt)
    {
        Hash = hash;
        Ast = ast;
        ExpiresAt = expiresAt;
    }
}


/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents the primary logic for parsing scripts, either generating a new parsed result
/// or retrieving a previously cached result, using the provided configuration and logger.
/// </summary>
internal sealed partial class scriptParser
{
    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;
    
    public static ConcurrentDictionary<string, ScriptCacheEntry> Cache { get; } = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public scriptParser(KahunaConfiguration configuration, ILogger<IKahuna> logger) : base(null)
    {
        this.configuration = configuration;
        this.logger = logger;
    }

    /// <summary>
    /// Parses or retrieves a script from the global cache
    /// </summary>
    /// <param name="inputBuffer"></param>
    /// <returns></returns>
    /// <exception cref="KahunaScriptException"></exception>
    /// <param name="hash">
    /// The caller's hash of the script, used only to look the script up. It is never used as the key an entry
    /// is stored under: an entry is always stored under the hash this method computes from the bytes it just
    /// parsed, so a wrong or invented hash can only miss the cache, never place one script's tree where
    /// another script's hash will find it. A caller that sends a hash that does not match its script simply
    /// reparses every time, which is its own cost alone.
    /// </param>
    public NodeAst Parse(ReadOnlySpan<byte> inputBuffer, string? hash)
    {
        if (!string.IsNullOrEmpty(hash) && Cache.TryGetValue(hash, out ScriptCacheEntry? cached))
        {
            cached.ExpiresAt = Environment.TickCount64 + (long)configuration.ScriptCacheExpiration.TotalMilliseconds;

            logger.LogScriptRetrievedFromCache(hash);

            return cached.Ast;
        }
        
        // Decode the UTF-8 script into a string and scan it directly. Every caller supplies
        // UTF-8 (SDK, protobuf payload, REST body), so a fixed decode is correct and avoids the
        // stream + StreamReader + StringBuilder buffering the stream-based scanner would use.
        NodeAst? root = ParseSource(Encoding.UTF8.GetString(inputBuffer));

        // Computed here rather than trusted, and only on a miss, so the hash cost is paid once per distinct
        // script instead of once per request. An absent caller hash means the caller does not want the script
        // cached at all, so nothing is stored and nothing is hashed.
        if (!string.IsNullOrEmpty(hash) && Cache.Count < configuration.ScriptCacheMaxEntries)
        {
            string computedHash = Blake3.Hasher.Hash(inputBuffer).ToString();

            logger.LogScriptAddedToCache(computedHash);

            Cache.TryAdd(computedHash, new(computedHash, root, Environment.TickCount64 + (long)configuration.ScriptCacheExpiration.TotalMilliseconds));
        }

        return root;
    }

    /// <summary>
    /// Parses a script already held as a string. In-process callers reach the parser this way, and this
    /// overload never consults or fills the cache: the cache is keyed by a hash of the encoded body, and a
    /// caller that has not encoded its script has not asked for its tree to be kept.
    /// </summary>
    public NodeAst Parse(string script)
    {
        return ParseSource(script);
    }

    /// <summary>
    /// Scans and parses the script text, then applies the depth limit. Everything above this method differs
    /// only in where the text came from and whether the result is cached.
    /// </summary>
    private NodeAst ParseSource(string source)
    {
        scriptScanner scanner = new();

        scanner.SetSource(source, 0);

        Scanner = scanner;

        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
        {
            string message = scanner.YYError;
            int line = scanner.YYErrorLine;
            int column = scanner.YYErrorColumn;

            if (line > 0 && column > 0)
            {
                message += $" at line {line}, column {column}";

                if (!string.IsNullOrEmpty(scanner.YYErrorToken))
                    message += $" near '{scanner.YYErrorToken}'";
            }

            throw new KahunaScriptException(message, line, column);
        }

        NodeAst? root = CurrentSemanticValue.n;

        EnsureDepthWithinLimit(root, configuration.MaxScriptDepth);

        return root;
    }

    /// <summary>
    /// Refuses a tree deeper than <paramref name="maxDepth"/>. Every walker over the tree descends one call
    /// frame per level, and a stack overflow cannot be caught, so without this a script of a few kilobytes
    /// aborts the whole process rather than failing one request.
    ///
    /// <para>The check itself must not recurse, for the same reason. It carries its own stack of pending
    /// nodes and stops at the first node past the limit, so a hostile tree costs one shallow pass and not a
    /// full traversal.</para>
    /// </summary>
    private static void EnsureDepthWithinLimit(NodeAst? root, int maxDepth)
    {
        if (root is null || maxDepth <= 0)
            return;

        Stack<(NodeAst Node, int Depth)> pending = new();

        pending.Push((root, 1));

        while (pending.Count > 0)
        {
            (NodeAst node, int depth) = pending.Pop();

            if (depth > maxDepth)
                throw new KahunaScriptException($"Script is nested too deeply: the limit is {maxDepth} levels", node.yyline);

            Push(pending, node.leftAst, depth);
            Push(pending, node.rightAst, depth);
            Push(pending, node.extendedOne, depth);
            Push(pending, node.extendedTwo, depth);
            Push(pending, node.extendedThree, depth);
            Push(pending, node.extendedFour, depth);
        }
    }

    private static void Push(Stack<(NodeAst, int)> pending, NodeAst? child, int parentDepth)
    {
        if (child is not null)
            pending.Push((child, parentDepth + 1));
    }
}

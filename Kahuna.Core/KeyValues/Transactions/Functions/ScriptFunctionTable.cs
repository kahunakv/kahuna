
using System.Collections.Frozen;

using Microsoft.Extensions.Logging;

using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Call counters for one user-defined function. A node reports them so an operator can see which
/// function is called, how often, and how much request time it takes.
///
/// <para>Many transactions call one function at once, so both counters move under
/// <see cref="Interlocked"/>. They are separate atomics rather than one guarded pair: a reader that
/// catches a count without its matching time is off by one call, which no operator decision turns on.
/// A lock here would put contention on the request path to buy nothing.</para>
/// </summary>
internal sealed class ScriptFunctionStats(string name)
{
    private long calls;

    private long elapsedTicks;

    /// <summary>The name scripts call this function by.</summary>
    public string Name { get; } = name;

    /// <summary>How many times the function was invoked since the node started.</summary>
    public long Calls => Interlocked.Read(ref calls);

    /// <summary>Total time spent inside the function, in <see cref="System.Diagnostics.Stopwatch"/> ticks.</summary>
    public long ElapsedTicks => Interlocked.Read(ref elapsedTicks);

    /// <summary>Total time spent inside the function, in milliseconds.</summary>
    public double ElapsedMilliseconds => ElapsedTicks * 1000d / System.Diagnostics.Stopwatch.Frequency;

    public void Record(long ticks)
    {
        Interlocked.Increment(ref calls);
        Interlocked.Add(ref elapsedTicks, ticks);
    }

    /// <summary>
    /// Counts a call whose duration was not measured. A node with the slow-call timer off does not
    /// read the clock on the request path, so <see cref="ElapsedTicks"/> stays at whatever the timed
    /// calls contributed. <see cref="Calls"/> stays exact either way.
    /// </summary>
    public void RecordCallOnly() => Interlocked.Increment(ref calls);
}

/// <summary>
/// A built-in that reads the running script's context as well as its arguments.
/// </summary>
internal delegate KeyValueExpressionResult ScriptBuiltInWithContext(ScriptTransactionContext context, NodeAst ast, List<KeyValueExpressionResult> arguments);

/// <summary>
/// One callable function: either a built-in or a user-defined one, never both.
///
/// <para>A built-in keeps the signature it always had, so its behavior is unchanged by the presence
/// of this table. A user-defined entry also carries the argument counts it accepts and the counters
/// that report it.</para>
/// </summary>
internal readonly struct ScriptFunctionEntry
{
    /// <summary>The built-in implementation, or null when this entry is user-defined or context-taking.</summary>
    public readonly Func<NodeAst, List<KeyValueExpressionResult>, KeyValueExpressionResult>? BuiltIn;

    /// <summary>The context-taking built-in implementation, or null for every other entry.</summary>
    public readonly ScriptBuiltInWithContext? BuiltInWithContext;

    /// <summary>The user-defined implementation, or null when this entry is a built-in.</summary>
    public readonly KahunaFunctionDelegate? Custom;

    /// <summary>Smallest accepted argument count, inclusive. Meaningful for a user-defined entry only.</summary>
    public readonly int MinArgs;

    /// <summary>Largest accepted argument count, inclusive, or -1 for variadic. User-defined entries only.</summary>
    public readonly int MaxArgs;

    /// <summary>Counters for a user-defined entry, or null for a built-in.</summary>
    public readonly ScriptFunctionStats? Stats;

    public ScriptFunctionEntry(Func<NodeAst, List<KeyValueExpressionResult>, KeyValueExpressionResult> builtIn)
    {
        BuiltIn = builtIn;
        BuiltInWithContext = null;
        Custom = null;
        MinArgs = 0;
        MaxArgs = -1;
        Stats = null;
    }

    public ScriptFunctionEntry(ScriptBuiltInWithContext builtIn)
    {
        BuiltIn = null;
        BuiltInWithContext = builtIn;
        Custom = null;
        MinArgs = 0;
        MaxArgs = -1;
        Stats = null;
    }

    public ScriptFunctionEntry(string name, KahunaFunctionDelegate custom, int minArgs, int maxArgs)
    {
        BuiltIn = null;
        BuiltInWithContext = null;
        Custom = custom;
        MinArgs = minArgs;
        MaxArgs = maxArgs;
        Stats = new(name);
    }
}

/// <summary>
/// Every function a node's scripts can call: the built-ins, plus whatever the host registered.
///
/// <para>The table is built once, when the node builds its script executor, and never changes. That
/// is what makes a call cheap and safe at the same time. The lookup is a
/// <see cref="FrozenDictionary{TKey,TValue}"/> probe with no lock, no torn read and no invalidation
/// of the parsed-script cache, so a user-defined function costs what a built-in costs.</para>
///
/// <para>A built-in always wins. The registry already refuses a name that collides with one
/// (<see cref="CallFunction.IsBuiltIn"/>), and the build below adds the built-ins last and asserts
/// that nothing was displaced, so a future change to the registry cannot quietly let a script's
/// meaning move.</para>
///
/// <para>The table carries the node name and the fingerprint because the error for an unknown
/// function is raised here, far from the raft handle that knows the node's identity. A registry that
/// differs between nodes is the failure this feature is most likely to hit in production, and an
/// error that names the node and its fingerprint turns it into a one-line diagnosis.</para>
/// </summary>
internal sealed class ScriptFunctionTable
{
    private readonly FrozenDictionary<string, ScriptFunctionEntry> entries;

    private readonly ScriptFunctionStats[] customStats;

    /// <summary>The node that evaluates scripts through this table.</summary>
    public string NodeName { get; }

    /// <summary>The registry fingerprint, or the empty-registry fingerprint when no function was registered.</summary>
    public string Fingerprint { get; }

    /// <summary>How many user-defined functions this table holds.</summary>
    public int CustomCount => customStats.Length;

    /// <summary>Logger used for an unexpected exception out of a user function, and for the slow-call warning.</summary>
    public ILogger<IKahuna> Logger { get; }

    /// <summary>
    /// A call slower than this many <see cref="System.Diagnostics.Stopwatch"/> ticks is logged as a
    /// warning. Zero disables the warning.
    /// </summary>
    public long SlowWarnTicks { get; }

    /// <summary>Configured slow-call threshold in milliseconds, for the warning message.</summary>
    public int SlowWarnMs { get; }

    public ScriptFunctionTable(KahunaFunctionRegistry? registry, string nodeName, int slowWarnMs, ILogger<IKahuna> logger)
    {
        NodeName = nodeName;
        Logger = logger;
        SlowWarnMs = slowWarnMs < 0 ? 0 : slowWarnMs;
        SlowWarnTicks = SlowWarnMs == 0 ? 0 : (long)(System.Diagnostics.Stopwatch.Frequency * (SlowWarnMs / 1000d));

        registry ??= new();

        KahunaFunctionEntry[] custom = registry.Freeze(out string fingerprint);

        Fingerprint = fingerprint;

        Dictionary<string, ScriptFunctionEntry> merged = new(CallFunction.BuiltInCount + custom.Length, StringComparer.Ordinal);
        List<ScriptFunctionStats> stats = new(custom.Length);

        foreach (KahunaFunctionEntry entry in custom)
        {
            ScriptFunctionEntry built = new(entry.Name, entry.Function, entry.MinArgs, entry.MaxArgs);

            merged[entry.Name] = built;
            stats.Add(built.Stats!);
        }

        // Built-ins go in last and must displace nothing. The registry already rejects a reserved
        // name, so reaching this throw means a change let a registration bypass that check — which
        // would silently change what an existing script means. Refusing to start is the only safe
        // answer, and it happens at node construction rather than mid-transaction.
        foreach (KeyValuePair<string, Func<NodeAst, List<KeyValueExpressionResult>, KeyValueExpressionResult>> builtIn in CallFunction.BuiltInFunctions)
        {
            if (merged.ContainsKey(builtIn.Key))
                throw new InvalidOperationException($"A user-defined function is registered under the built-in name '{builtIn.Key}'. Built-in names are reserved");

            merged[builtIn.Key] = new(builtIn.Value);
        }

        // The context-taking built-ins are reserved on exactly the same terms, and land in the same table,
        // so nothing downstream has to know which of the two kinds a name resolves to.
        foreach (KeyValuePair<string, ScriptBuiltInWithContext> builtIn in CallFunction.ContextualBuiltInFunctions)
        {
            if (merged.ContainsKey(builtIn.Key))
                throw new InvalidOperationException($"A user-defined function is registered under the built-in name '{builtIn.Key}'. Built-in names are reserved");

            merged[builtIn.Key] = new(builtIn.Value);
        }

        entries = merged.ToFrozenDictionary(StringComparer.Ordinal);
        customStats = stats.ToArray();
    }

    /// <summary>Looks a function up by the name the script used. Ordinal and case-sensitive.</summary>
    public bool TryGet(string name, out ScriptFunctionEntry entry) => entries.TryGetValue(name, out entry);

    /// <summary>The counters of every user-defined function, for the metrics surface.</summary>
    public IReadOnlyList<ScriptFunctionStats> CustomStats => customStats;

    /// <summary>
    /// The message for a function this node does not have. It names the node and the fingerprint,
    /// because the usual cause is one node of a cluster that loaded a different extension build, and
    /// without those two facts the failure reads as an intermittent error on a random request.
    /// </summary>
    public string DescribeUndefined(string name)
    {
        return $"Undefined function '{name}' on node {NodeName} (functions {Fingerprint})";
    }
}

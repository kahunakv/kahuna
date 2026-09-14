
using System.Security.Cryptography;
using System.Text;

using Kahuna.Server.KeyValues.Transactions.Functions;

namespace Kahuna.Extensibility;

/// <summary>One registered function: its name, its implementation, and the argument counts it accepts.</summary>
internal readonly record struct KahunaFunctionEntry(string Name, KahunaFunctionDelegate Function, int MinArgs, int MaxArgs);

/// <summary>
/// The set of user-defined functions a node makes callable from Kahuna script.
///
/// <para>A host fills the registry before it builds the node. The node then freezes it into an
/// immutable lookup table, and a later <see cref="Register"/> throws. Nothing registers a function
/// over the wire, and no script can add, replace, or list one: a client can only call what the
/// operator installed.</para>
///
/// <para>The registry is per process. Kahuna does not replicate it. Every node of a cluster must
/// register an identical set, the same way every node must run the same binary. A node that does not
/// have a function answers a script that calls it with <c>Errored</c>, and names itself and its
/// <see cref="Fingerprint"/> in the message so the wrong node is immediately obvious. A node that
/// lacks the function is still a correct follower: it applies, restores, and serves values a
/// function on another node produced, because replication carries the result and never the call.</para>
/// </summary>
public sealed class KahunaFunctionRegistry
{
    private readonly Lock mutex = new();

    private readonly Dictionary<string, KahunaFunctionEntry> entries = new(StringComparer.Ordinal);

    private string? fingerprint;

    private bool frozen;

    /// <summary>
    /// Registers a function under a name a script can call.
    ///
    /// <para>Every rule below is checked here, at startup, and never at script time. A deployment
    /// therefore fails to start rather than failing one transaction at an unpredictable moment.</para>
    /// </summary>
    /// <param name="name">
    /// The name scripts call. It must match the script identifier shape <c>[a-zA-Z_][a-zA-Z0-9_]*</c>,
    /// because the grammar cannot reach any other shape. Matching is ordinal and case-sensitive.
    /// Prefix a name with an application tag, such as <c>acme_crc32</c>, so a built-in added in a
    /// later release cannot collide with the scripts a deployment already runs.
    /// </param>
    /// <param name="function">The implementation. See <see cref="KahunaFunctionDelegate"/> for the rules it must obey.</param>
    /// <param name="minArgs">Smallest accepted argument count, inclusive.</param>
    /// <param name="maxArgs">Largest accepted argument count, inclusive. Pass -1 for a variadic function.</param>
    /// <returns>This registry, so registrations can be chained.</returns>
    /// <exception cref="ArgumentException">
    /// The name is not a script identifier, the name is a built-in or one of its aliases, the name is
    /// already registered, or the argument bounds are not sane.
    /// </exception>
    /// <exception cref="InvalidOperationException">The node already froze this registry.</exception>
    public KahunaFunctionRegistry Register(string name, KahunaFunctionDelegate function, int minArgs = 0, int maxArgs = -1)
    {
        ArgumentNullException.ThrowIfNull(function);

        if (!IsScriptIdentifier(name))
            throw new ArgumentException($"'{name}' is not a valid function name: a name must match [a-zA-Z_][a-zA-Z0-9_]*, or no script could call it", nameof(name));

        // Reserved names are read from the engine's own table rather than from a list kept here, so a
        // built-in added later becomes reserved with it. A copied list would drift, and the drift
        // would silently let a deployment shadow a built-in and change what its scripts mean.
        if (CallFunction.IsBuiltIn(name))
            throw new ArgumentException($"'{name}' is a built-in function name and is reserved, so scripts keep one fixed meaning for it", nameof(name));

        if (minArgs < 0)
            throw new ArgumentOutOfRangeException(nameof(minArgs), minArgs, "minArgs cannot be negative");

        if (maxArgs >= 0 && maxArgs < minArgs)
            throw new ArgumentOutOfRangeException(nameof(maxArgs), maxArgs, $"maxArgs must be -1 for a variadic function, or at least minArgs ({minArgs})");

        lock (mutex)
        {
            if (frozen)
                throw new InvalidOperationException($"Cannot register '{name}': this registry was already frozen by a node. Register every function before the node is constructed");

            if (!entries.TryAdd(name, new(name, function, minArgs, maxArgs)))
                throw new ArgumentException($"'{name}' is already registered. A second registration is rejected rather than silently replacing the first", nameof(name));

            fingerprint = null;
        }

        return this;
    }

    /// <summary>Whether a name is registered here. Built-in names are not reported by this method.</summary>
    public bool Contains(string name)
    {
        lock (mutex)
            return entries.ContainsKey(name);
    }

    /// <summary>How many functions are registered.</summary>
    public int Count
    {
        get
        {
            lock (mutex)
                return entries.Count;
        }
    }

    /// <summary>The registered names, in ordinal order.</summary>
    public IReadOnlyCollection<string> Names
    {
        get
        {
            lock (mutex)
            {
                string[] names = new string[entries.Count];
                entries.Keys.CopyTo(names, 0);
                Array.Sort(names, StringComparer.Ordinal);

                return names;
            }
        }
    }

    /// <summary>
    /// A stable short hash over the registered surface: the first 16 hex characters of a SHA-256 over
    /// the ordinally sorted <c>name:minArgs:maxArgs</c> lines.
    ///
    /// <para>It exists so an operator can tell two nodes apart. Registration order does not change it,
    /// and a changed name or a changed argument count does. It appears in the startup log, on the
    /// node's metrics, and in the error a script gets when it calls a function this node does not
    /// have. An empty registry hashes to the fixed value of the empty input.</para>
    /// </summary>
    public string Fingerprint
    {
        get
        {
            lock (mutex)
                return fingerprint ??= ComputeFingerprint();
        }
    }

    /// <summary>
    /// Closes the registry and returns its contents. The node calls this once, when it builds its
    /// lookup table. Registration after this point throws.
    /// </summary>
    internal KahunaFunctionEntry[] Freeze(out string frozenFingerprint)
    {
        lock (mutex)
        {
            frozen = true;
            frozenFingerprint = fingerprint ??= ComputeFingerprint();

            KahunaFunctionEntry[] frozenEntries = new KahunaFunctionEntry[entries.Count];
            entries.Values.CopyTo(frozenEntries, 0);

            return frozenEntries;
        }
    }

    /// <summary>Computes the fingerprint. The caller holds the lock.</summary>
    private string ComputeFingerprint()
    {
        string[] names = new string[entries.Count];
        entries.Keys.CopyTo(names, 0);
        Array.Sort(names, StringComparer.Ordinal);

        StringBuilder builder = new(names.Length * 24);

        foreach (string name in names)
        {
            KahunaFunctionEntry entry = entries[name];

            builder.Append(name).Append(':').Append(entry.MinArgs).Append(':').Append(entry.MaxArgs).Append('\n');
        }

        Span<byte> hash = stackalloc byte[32];

        SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()), hash);

        return Convert.ToHexStringLower(hash[..8]);
    }

    /// <summary>
    /// Whether a name matches the identifier shape the script lexer accepts. A name that does not
    /// match could never be reached from a script, so it is rejected at registration.
    /// </summary>
    private static bool IsScriptIdentifier(string? name)
    {
        if (string.IsNullOrEmpty(name))
            return false;

        char first = name[0];

        if (!char.IsAsciiLetter(first) && first != '_')
            return false;

        for (int i = 1; i < name.Length; i++)
        {
            char current = name[i];

            if (!char.IsAsciiLetterOrDigit(current) && current != '_')
                return false;
        }

        return true;
    }
}

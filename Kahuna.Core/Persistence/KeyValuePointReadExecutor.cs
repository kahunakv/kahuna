
using System.Runtime.CompilerServices;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence.Backend;
using Kommander.WAL.IO;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Batch executor for current-revision point reads
/// (<see cref="IPersistenceBackend.GetKeyValue"/>) submitted through the backend read
/// scheduler. When a drained scheduler cycle contains several point reads against the same
/// executor instance, they are served by one <see cref="IPersistenceBackend.GetKeyValues"/>
/// call (a single RocksDB <c>MultiGet</c>) instead of N individual gets — compressing
/// cold-read bursts.
///
/// <para>
/// The scheduler groups operations by executor <b>reference identity</b>, so there must be
/// exactly one instance per backend for coalescing to work across all key/value actors.
/// <see cref="For"/> memoizes per backend instance; actors resolve their constructor through
/// DI with a fixed positional argument list, so a shared instance is obtained here rather
/// than threaded through every actor constructor.
/// </para>
///
/// <para>
/// Safe to batch because these disk reads are mutually independent: dirty in-memory entries
/// are never served from disk, so the backend is only consulted when it is authoritative —
/// the same property that already allows concurrent per-partition dispatch in the read
/// scheduler.
/// </para>
/// </summary>
internal sealed class KeyValuePointReadExecutor : IReadBatchExecutor<string, KeyValueEntry?>
{
    private static readonly ConditionalWeakTable<IPersistenceBackend, KeyValuePointReadExecutor> instances = new();

    private readonly IPersistenceBackend backend;

    private KeyValuePointReadExecutor(IPersistenceBackend backend)
    {
        this.backend = backend;
    }

    /// <summary>
    /// Returns the single executor for <paramref name="backend"/>, creating it on first use.
    /// The table holds the backend weakly, so a disposed backend's executor is collectable.
    /// </summary>
    internal static KeyValuePointReadExecutor For(IPersistenceBackend backend)
    {
        return instances.GetValue(backend, static b => new(b));
    }

    /// <inheritdoc/>
    public KeyValueEntry?[] ExecuteBatch(string[] args)
    {
        return backend.GetKeyValues(args);
    }
}

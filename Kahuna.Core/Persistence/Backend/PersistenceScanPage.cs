
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// One page of a whole-family key-value scan (<see cref="IPersistenceBackend.ScanKeyValues"/>).
/// <see cref="NextCursor"/> is the opaque, backend-owned resume token for the next page; null
/// means the scan is complete. A page may be short — or even empty — while the cursor is still
/// non-null (e.g. a sharded backend advancing to its next shard), so callers must iterate until
/// the cursor is null rather than stopping at the first short page.
/// </summary>
internal sealed record KeyValueScanPage(List<(string Key, ReadOnlyKeyValueEntry Entry)> Items, string? NextCursor);

/// <summary>
/// One page of a whole-family lock scan (<see cref="IPersistenceBackend.ScanLocks"/>). Cursor
/// semantics are identical to <see cref="KeyValueScanPage"/>.
/// </summary>
internal sealed record LockScanPage(List<(string Resource, LockEntry Entry)> Items, string? NextCursor);

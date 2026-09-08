
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Result of <see cref="IPersistenceBackend.GetKeyValueWithRecentRevisions"/>: the current head of a
/// key (null when the key has no current row) and its newest archived revisions, newest first. The
/// list holds only revisions that exist in the store — a pruned or never-written revision number is
/// simply absent, so the list may be shorter than requested or non-contiguous.
/// </summary>
/// <param name="Head">The key's current row, or null.</param>
/// <param name="RecentRevisions">Archived revisions below the head, newest first.</param>
internal sealed record KeyValueHydration(KeyValueEntry? Head, List<KeyValueEntry> RecentRevisions);

using System.Collections.Concurrent;

namespace Kahuna.TestStorageExtension.Store;

/// <summary>
/// A thread-safe map from keys to rows, the storage of the fixture engine. It lives in its own assembly so
/// the engine has a dependency that only the extension's directory holds.
/// </summary>
public sealed class RowStore<TRow> where TRow : class
{
    private readonly ConcurrentDictionary<string, TRow> rows = new(StringComparer.Ordinal);

    /// <summary>Stores <paramref name="row"/> under <paramref name="key"/>, replacing what was there.</summary>
    public void Put(string key, TRow row) => rows[key] = row;

    /// <summary>The row under <paramref name="key"/>, or null.</summary>
    public TRow? Get(string key) => rows.GetValueOrDefault(key);

    /// <summary>Every key and row, in ordinal key order.</summary>
    public List<(string Key, TRow Row)> All() =>
        [.. rows.OrderBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => (pair.Key, pair.Value))];
}

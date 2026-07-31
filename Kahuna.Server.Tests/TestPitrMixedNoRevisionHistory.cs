
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Transition-matrix coverage for keys that mix revisioned and SetNoRevision writes, on memory,
/// SQLite, and RocksDB. A SetNoRevision write retains no history row, so the as-of checkpoint must
/// use the current value when it is at/before the cut (a revisioned→no-revision key must not reset to
/// an older revision) and fail closed when the cut's boundary is an overwritten no-revision value
/// that cannot be reconstructed (a no-revision→revisioned key must not silently omit the key).
/// </summary>
public sealed class TestPitrMixedNoRevisionHistory : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_mixnorev_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private static HLCTimestamp T(long ms) => new(0, ms, 0);

    private sealed record Write(string Value, long Revision, long PhysicalMs, bool NoRevision);

    private static PersistenceRequestItem ToItem(string key, Write w) =>
        new(key, Encoding.UTF8.GetBytes(w.Value), w.Revision,
            0, 0, 0, 0, w.PhysicalMs, 0, 0, w.PhysicalMs, 0, (int)KeyValueState.Set, noRevision: w.NoRevision);

    private enum Kind { Absent, FailClosed, Present }

    /// <summary>Applies the writes to a fresh backend, checkpoints as-of <paramref name="cut"/>, and reports what the key resolves to.</summary>
    private (Kind Kind, string? Value) Probe(string storage, string key, IReadOnlyList<Write> writes, HLCTimestamp cut)
    {
        string id = Guid.NewGuid().ToString("N")[..8];
        string baseDir = Path.Combine(_tempRoot, storage + "_" + id);
        Directory.CreateDirectory(baseDir);

        IPersistenceBackend backend = storage switch
        {
            "memory"  => new MemoryPersistenceBackend(),
            "sqlite"  => new SqlitePersistenceBackend(baseDir, "v1"),
            "rocksdb" => new RocksDbPersistenceBackend(baseDir, "v1"),
            _ => throw new ArgumentException(storage)
        };

        try
        {
            foreach (Write w in writes)
                backend.StoreKeyValues([ToItem(key, w)]);

            string cpDir = storage == "rocksdb" ? Path.Combine(baseDir, "cp") : Path.Combine(baseDir, "cp_" + id);
            try
            {
                backend.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: cut);
            }
            catch (ExactCheckpointUnavailableException)
            {
                return (Kind.FailClosed, null);
            }

            IPersistenceBackend reopened = storage switch
            {
                "memory"  => MemoryPersistenceBackend.OpenCheckpoint(cpDir),
                "sqlite"  => new SqlitePersistenceBackend(cpDir, "v1"),
                "rocksdb" => new RocksDbPersistenceBackend(baseDir, "cp"),
                _ => throw new ArgumentException(storage)
            };
            try
            {
                KeyValueEntry? e = reopened.GetKeyValue(key);
                return e is null ? (Kind.Absent, null) : (Kind.Present, Encoding.UTF8.GetString(e.Value!));
            }
            finally { (reopened as IDisposable)?.Dispose(); }
        }
        finally { (backend as IDisposable)?.Dispose(); }
    }

    private void AssertValue(string storage, string key, IReadOnlyList<Write> writes, HLCTimestamp cut, string expected)
    {
        (Kind kind, string? value) = Probe(storage, key, writes, cut);
        Assert.Equal(Kind.Present, kind);
        Assert.Equal(expected, value);
    }

    private void AssertAbsent(string storage, string key, IReadOnlyList<Write> writes, HLCTimestamp cut)
    {
        (Kind kind, _) = Probe(storage, key, writes, cut);
        Assert.Equal(Kind.Absent, kind);
    }

    private void AssertFailClosed(string storage, string key, IReadOnlyList<Write> writes, HLCTimestamp cut)
    {
        (Kind kind, _) = Probe(storage, key, writes, cut);
        Assert.Equal(Kind.FailClosed, kind);
    }

    // ── revisioned (@50) then no-revision (@100) ───────────────────────────────────────────────
    // The no-revision current value is the exact state at/after 100; before 100 the retained
    // revision is the state; before 50 the key does not exist.

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void RevisionedThenNoRevision(string storage)
    {
        const string key = "mix/rev-then-norev";
        List<Write> writes =
        [
            new("R", Revision: 1, PhysicalMs: 50,  NoRevision: false),
            new("N", Revision: 2, PhysicalMs: 100, NoRevision: true),
        ];

        AssertAbsent(storage, key, writes, T(25));   // before the key existed
        AssertValue(storage, key, writes, T(50),  "R"); // at the revisioned write
        AssertValue(storage, key, writes, T(75),  "R"); // between: retained revision
        AssertValue(storage, key, writes, T(100), "N"); // at the no-revision write: current value
        AssertValue(storage, key, writes, T(150), "N"); // after: current value
    }

    // ── no-revision (@50) then revisioned (@100) ───────────────────────────────────────────────
    // The no-revision value at 50 is overwritten by the revisioned write and cannot be
    // reconstructed, so any cut in [50, 100) must fail closed; at/after 100 the revisioned current
    // value is exact; before 50 the key does not exist.

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void NoRevisionThenRevisioned(string storage)
    {
        const string key = "mix/norev-then-rev";
        List<Write> writes =
        [
            new("N", Revision: 1, PhysicalMs: 50,  NoRevision: true),
            new("R", Revision: 2, PhysicalMs: 100, NoRevision: false),
        ];

        AssertAbsent(storage, key, writes, T(25));      // before the key existed
        AssertFailClosed(storage, key, writes, T(50));  // boundary is the overwritten no-revision value
        AssertFailClosed(storage, key, writes, T(75));  // same
        AssertValue(storage, key, writes, T(100), "R"); // at the revisioned write: current value
        AssertValue(storage, key, writes, T(150), "R"); // after: current value
    }
}

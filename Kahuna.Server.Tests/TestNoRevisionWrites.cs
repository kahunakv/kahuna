using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Tests;

/// <summary>
/// Backend-level tests for the no-revision write flag. A <c>PersistenceRequestItem</c> carrying
/// <c>NoRevision = true</c> must update only the current value and never archive a historical
/// revision row, while normal (non-flag) writes to the same key keep their revision rows intact.
/// </summary>
public sealed class TestNoRevisionWrites
{
    private static PersistenceRequestItem Item(string key, long revision, long physical, bool noRevision) =>
        new(key,
            Encoding.UTF8.GetBytes("v" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: physical, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set,
            noRevision: noRevision);

    private static string TempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_norev_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static string ValueOf(KeyValueEntry? entry) => Encoding.UTF8.GetString(entry!.Value!);

    /// <summary>
    /// A no-revision SET writes only the current value: the latest GET reflects it, but no
    /// <c>&lt;key&gt;~&lt;revision&gt;</c> row is created for that revision. A preceding normal SET's
    /// revision row survives, and the revision counter still advances.
    /// </summary>
    private static void AssertNoRevisionRowSuppressed(IPersistenceBackend backend)
    {
        // Normal write at revision 0 — creates both ~CURRENT and ~0.
        backend.StoreKeyValues([Item("k", 0, 100, noRevision: false)]);
        // No-revision write at revision 1 — must update ~CURRENT only, no ~1 row.
        backend.StoreKeyValues([Item("k", 1, 200, noRevision: true)]);

        // Current value reflects the latest (no-revision) write and the counter advanced to 1.
        KeyValueEntry? current = backend.GetKeyValue("k");
        Assert.NotNull(current);
        Assert.Equal(1, current!.Revision);
        Assert.Equal("v1", ValueOf(current));

        // The no-revision revision row was never written.
        Assert.Null(backend.GetKeyValueRevision("k", 1));

        // The earlier normal write's revision row is untouched.
        KeyValueEntry? rev0 = backend.GetKeyValueRevision("k", 0);
        Assert.NotNull(rev0);
        Assert.Equal("v0", ValueOf(rev0));
    }

    [Fact]
    public void TestRocksDbNoRevisionRowSuppressed()
    {
        using RocksDbPersistenceBackend backend = new(TempPath(), "v1");
        AssertNoRevisionRowSuppressed(backend);
    }

    [Fact]
    public void TestSqliteNoRevisionRowSuppressed()
    {
        using SqlitePersistenceBackend backend = new(TempPath(), "v1");
        AssertNoRevisionRowSuppressed(backend);
    }

    [Fact]
    public void TestMemoryNoRevisionRowSuppressed()
    {
        using MemoryPersistenceBackend backend = new();
        AssertNoRevisionRowSuppressed(backend);
    }
}

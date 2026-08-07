using Kahuna.Server.Configuration;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers how a node decides where to keep its data when the operator did not say. The invariant
/// that matters: an unset path must never reach a backend as an empty string, because every backend
/// composes its directory as "{path}/{revision}" and would land at the root of the filesystem.
///
/// The environment variables consulted here are process-global, so every test that mutates one
/// restores it; xUnit runs the tests in a single class sequentially, and no other test class reads
/// these variables.
/// </summary>
public sealed class TestDataPathResolution
{
    [Fact]
    public void UnsetStoragePathResolvesUnderTheKahunaHomeRoot()
    {
        using EnvironmentVariable home = EnvironmentVariable.Set(DataPathResolver.HomeVariable, Path.Combine(Path.GetTempPath(), "kahuna-home-probe"));

        string resolved = DataPathResolver.ResolveStoragePath(null);

        Assert.Equal(Path.Combine(home.Value!, "data"), resolved);
    }

    [Fact]
    public void UnsetWalPathResolvesUnderTheKahunaHomeRoot()
    {
        using EnvironmentVariable home = EnvironmentVariable.Set(DataPathResolver.HomeVariable, Path.Combine(Path.GetTempPath(), "kahuna-home-probe"));

        string resolved = DataPathResolver.ResolveWalPath(null);

        Assert.Equal(Path.Combine(home.Value!, "wal"), resolved);
    }

    [Fact]
    public void StorageAndWalResolveToDistinctDirectories()
    {
        using EnvironmentVariable home = EnvironmentVariable.Set(DataPathResolver.HomeVariable, Path.Combine(Path.GetTempPath(), "kahuna-home-probe"));

        Assert.NotEqual(DataPathResolver.ResolveStoragePath(null), DataPathResolver.ResolveWalPath(null));
    }

    [Theory]
    [InlineData("/var/lib/kahuna/data")]
    [InlineData("./relative-data")]
    public void ConfiguredPathIsUsedVerbatim(string configured)
    {
        // The container entrypoint and cluster run scripts all pass explicit paths; resolution must
        // never rewrite them, or an operator's data would silently move on upgrade.
        using EnvironmentVariable home = EnvironmentVariable.Set(DataPathResolver.HomeVariable, Path.Combine(Path.GetTempPath(), "kahuna-home-probe"));

        Assert.Equal(configured, DataPathResolver.ResolveStoragePath(configured));
        Assert.Equal(configured, DataPathResolver.ResolveWalPath(configured));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ResolutionNeverYieldsAnEmptyOrRelativePath(string? configured)
    {
        // The regression guard for the failure this resolver exists to prevent: an empty path
        // composes to "/{revision}" at the root of the filesystem.
        using EnvironmentVariable home = EnvironmentVariable.Clear(DataPathResolver.HomeVariable);

        string storage = DataPathResolver.ResolveStoragePath(configured);
        string wal = DataPathResolver.ResolveWalPath(configured);

        Assert.False(string.IsNullOrWhiteSpace(storage));
        Assert.False(string.IsNullOrWhiteSpace(wal));
        Assert.True(Path.IsPathRooted(storage));
        Assert.True(Path.IsPathRooted(wal));

        // "/v1" is what the old empty-string default produced; the resolved root must be a real
        // directory beneath the user's own space, not a single segment at the filesystem root.
        Assert.NotEqual(Path.GetPathRoot(storage), Path.GetDirectoryName(storage));
    }

    [Fact]
    public void KahunaHomeOverridesThePlatformDefaultRoot()
    {
        string overridden = Path.Combine(Path.GetTempPath(), "kahuna-home-override");

        using EnvironmentVariable cleared = EnvironmentVariable.Clear(DataPathResolver.HomeVariable);

        string defaultRoot = DataPathResolver.ResolveRoot();

        using EnvironmentVariable home = EnvironmentVariable.Set(DataPathResolver.HomeVariable, overridden);

        Assert.Equal(Path.GetFullPath(overridden), DataPathResolver.ResolveRoot());
        Assert.NotEqual(defaultRoot, DataPathResolver.ResolveRoot());
    }

    [Fact]
    public void UnsetStorageRevisionResolvesToAStableValue()
    {
        // Left unset, the embedded node mints a fresh GUID per boot — an isolated scratch keyspace
        // for in-process tests, but for a server it means opening an empty database on every start
        // and leaking the previous one. Two resolutions must agree, or restarts lose data.
        string first = DataPathResolver.ResolveStorageRevision(null);
        string second = DataPathResolver.ResolveStorageRevision("");

        Assert.Equal(DataPathResolver.DefaultStorageRevision, first);
        Assert.Equal(first, second);
        Assert.False(Guid.TryParse(first, out _));
    }

    [Fact]
    public void ConfiguredStorageRevisionIsUsedVerbatim()
    {
        Assert.Equal("v2", DataPathResolver.ResolveStorageRevision("v2"));
    }

    [Fact]
    public void InMemoryBackendsAreRecognisedRegardlessOfCasing()
    {
        // An in-memory backend owns no directory, so no path is resolved or created for it.
        Assert.True(DataPathResolver.IsInMemory("memory"));
        Assert.True(DataPathResolver.IsInMemory("Memory"));
        Assert.False(DataPathResolver.IsInMemory("rocksdb"));
        Assert.False(DataPathResolver.IsInMemory("sqlite"));
        Assert.False(DataPathResolver.IsInMemory(null));
    }

    /// <summary>
    /// Sets a process environment variable for the duration of a test and restores whatever was
    /// there before, so a failing assertion cannot leak state into the rest of the run.
    /// </summary>
    private sealed class EnvironmentVariable : IDisposable
    {
        private readonly string name;

        private readonly string? original;

        public string? Value { get; }

        private EnvironmentVariable(string name, string? value)
        {
            this.name = name;
            original = Environment.GetEnvironmentVariable(name);
            Value = value;
            Environment.SetEnvironmentVariable(name, value);
        }

        public static EnvironmentVariable Set(string name, string value) => new(name, value);

        public static EnvironmentVariable Clear(string name) => new(name, null);

        public void Dispose() => Environment.SetEnvironmentVariable(name, original);
    }
}

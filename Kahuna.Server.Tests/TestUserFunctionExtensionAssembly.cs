using System.Text;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers <c>--extension-assembly</c>: the only way an operator running the shipped binary can add a
/// function without recompiling.
///
/// <para>Every failure here must stop startup with its own actionable message. A node that came up
/// missing one function would answer scripts that call it with <c>Errored</c> while its peers
/// answered normally — a node-dependent, intermittent failure that is very hard to trace back to a
/// deployment mistake.</para>
///
/// <para>These tests set a process-wide environment variable to drive the fixture assembly's failure
/// modes, so they must not run at the same time as each other. xUnit runs the methods of one class
/// in sequence, and no other class touches that variable.</para>
/// </summary>
public sealed class TestUserFunctionExtensionAssembly
{
    private const string ModeVariable = "KAHUNA_TEST_EXTENSION_MODE";

    private readonly ILoggerFactory loggerFactory;

    public TestUserFunctionExtensionAssembly(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    /// <summary>
    /// Where the fixture assembly is built. It is referenced without compilation, so it lands in its
    /// own output directory rather than this one; the path is derived from this assembly's, so it
    /// follows the configuration and target framework of the run.
    /// </summary>
    private static string FixturePath()
    {
        string baseDirectory = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);

        string path = baseDirectory.Replace(
            Path.DirectorySeparatorChar + "Kahuna.Server.Tests" + Path.DirectorySeparatorChar,
            Path.DirectorySeparatorChar + "Kahuna.TestExtension" + Path.DirectorySeparatorChar,
            StringComparison.Ordinal);

        return Path.Combine(path, "Kahuna.TestExtension.dll");
    }

    /// <summary>An assembly that is perfectly loadable but publishes no provider.</summary>
    private static string AssemblyWithoutAProvider() => Path.Combine(AppContext.BaseDirectory, "Kahuna.Shared.dll");

    private static IDisposable Mode(string? mode) => new ModeScope(mode);

    private sealed class ModeScope : IDisposable
    {
        private readonly string? previous;

        public ModeScope(string? mode)
        {
            previous = Environment.GetEnvironmentVariable(ModeVariable);
            Environment.SetEnvironmentVariable(ModeVariable, mode);
        }

        public void Dispose() => Environment.SetEnvironmentVariable(ModeVariable, previous);
    }

    [Fact]
    public void TestNoFlagLoadsNothing()
    {
        // Absent the flag the feature is entirely inert: no assembly is opened, no context is created
        // and the node's table holds only the built-ins.
        KahunaFunctionRegistry none = ExtensionAssemblyLoader.Load(null, NullLogger.Instance);

        Assert.Equal(0, none.Count);

        KahunaFunctionRegistry empty = ExtensionAssemblyLoader.Load([], NullLogger.Instance);

        Assert.Equal(0, empty.Count);
        Assert.Equal(none.Fingerprint, empty.Fingerprint);
    }

    [Fact]
    public void TestValidAssemblyRegistersItsFunctions()
    {
        using IDisposable _ = Mode(null);

        string fixture = FixturePath();

        Assert.True(File.Exists(fixture), $"the fixture assembly was not built at {fixture}");

        KahunaFunctionRegistry registry = ExtensionAssemblyLoader.Load([fixture], NullLogger.Instance);

        Assert.Equal(2, registry.Count);
        Assert.True(registry.Contains("ext_double"));
        Assert.True(registry.Contains("ext_greet"));
    }

    [Fact]
    public async Task TestLoadedFunctionsAreCallableThroughTheScriptPath()
    {
        using IDisposable _ = Mode(null);

        CancellationToken ct = TestContext.Current.CancellationToken;

        KahunaFunctionRegistry registry = ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance);

        // The same handover the server makes: the loaded registry becomes the node's.
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            Functions = registry
        };

        await using EmbeddedKahunaNode node = new(options, loggerFactory);

        await node.StartAsync(ct);

        KeyValueTransactionResult doubled = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("RETURN ext_double(21)"), null, null);

        Assert.True(doubled.Type == KeyValueResponseType.Get, $"{doubled.Type}: {doubled.Reason}");
        Assert.Equal("42", Encoding.UTF8.GetString(doubled.Value ?? []));

        KeyValueTransactionResult greeted = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("RETURN ext_greet('world')"), null, null);

        Assert.Equal("hello world", Encoding.UTF8.GetString(greeted.Value ?? []));
    }

    [Fact]
    public void TestMissingFileFailsWithItsOwnMessage()
    {
        string missing = Path.Combine(Path.GetTempPath(), "kahuna-not-here-" + Guid.NewGuid().ToString("N") + ".dll");

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ExtensionAssemblyLoader.Load([missing], NullLogger.Instance));

        Assert.Contains("does not exist", ex.Message, StringComparison.Ordinal);
        Assert.Contains(Path.GetFileName(missing), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestEmptyPathFailsWithItsOwnMessage()
    {
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ExtensionAssemblyLoader.Load(["  "], NullLogger.Instance));

        Assert.Contains("empty path", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestAssemblyWithoutAProviderFailsWithItsOwnMessage()
    {
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ExtensionAssemblyLoader.Load([AssemblyWithoutAProvider()], NullLogger.Instance));

        Assert.Contains("publishes no functions", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(IKahunaFunctionProvider), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestProviderConstructorThatThrowsFailsWithItsOwnMessage()
    {
        using IDisposable _ = Mode("ctor-throw");

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance));

        Assert.Contains("constructor of provider", ex.Message, StringComparison.Ordinal);
        Assert.Contains("on purpose", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestProviderRegisterThatThrowsFailsWithItsOwnMessage()
    {
        using IDisposable _ = Mode("register-throw");

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance));

        Assert.Contains("while registering", ex.Message, StringComparison.Ordinal);
        Assert.Contains("on purpose", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestReservedNameFailsWithItsOwnMessage()
    {
        using IDisposable _ = Mode("reserved-name");

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance));

        Assert.Contains("unacceptable function", ex.Message, StringComparison.Ordinal);
        Assert.Contains("reserved", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestTwoAssembliesRegisteringTheSameNameFail()
    {
        using IDisposable _ = Mode(null);

        string fixture = FixturePath();

        // The same assembly twice is the simplest shape of the real mistake: two extension files that
        // both publish one name. The second registration must be rejected, not silently win.
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ExtensionAssemblyLoader.Load([fixture, fixture], NullLogger.Instance));

        Assert.Contains("unacceptable function", ex.Message, StringComparison.Ordinal);
        Assert.Contains("already registered", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestLoadingLogsThePathAndHash()
    {
        using IDisposable _ = Mode(null);

        RecordingLogger logger = new();

        ExtensionAssemblyLoader.Load([FixturePath()], logger);

        // An operator confirms every node loaded the same artifact by comparing this hash.
        string line = Assert.Single(logger.Lines);

        Assert.Contains("Kahuna.TestExtension.dll", line, StringComparison.Ordinal);
        Assert.Contains("sha256", line, StringComparison.Ordinal);
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Lines.Add(formatter(state, exception));
        }
    }
}

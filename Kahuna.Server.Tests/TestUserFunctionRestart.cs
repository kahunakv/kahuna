using System.Text;

using Microsoft.Extensions.Logging;

using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// Proves that a value a user-defined function produced outlives the function itself.
///
/// <para>The registry is not replicated and is not persisted. If restoring a value needed the
/// function that made it, removing a function would make old data unreadable, and an operator could
/// never retire one. What is written is the materialized value, so restore never re-invokes
/// anything. This test removes the function on restart and reads the value back.</para>
/// </summary>
public sealed class TestUserFunctionRestart
{
    private readonly ILoggerFactory loggerFactory;

    public TestUserFunctionRestart(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(path);

        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup.
        }
    }

    /// <summary>
    /// A node whose stores survive a restart. The revision strings are fixed, not generated: an empty
    /// revision defaults to a fresh identifier per construction, which would reopen empty stores and
    /// make the test pass for the wrong reason.
    /// </summary>
    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath, Action<EmbeddedKahunaOptions> configure)
    {
        EmbeddedKahunaOptions options = new()
        {
            InitialPartitions = 1,
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "sqlite",
            StoragePath = storagePath,
            StorageRevision = "user-function-restart",
            WalStorage = "sqlite",
            WalPath = walPath,
            WalRevision = "user-function-restart-wal",
            WalSyncWrites = true
        };

        configure(options);

        return options;
    }

    [Fact]
    public async Task TestValueWrittenByAFunctionRestoresWithoutTheFunction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-userfn-store-");
        string walPath = CreateTempDir("kahuna-userfn-wal-");

        const string Key = "userfn/restored";

        try
        {
            string beforeFingerprint;

            await using (EmbeddedKahunaNode node = new(
                PersistentOptions(storagePath, walPath, o =>
                    o.Functions.Register(
                        "acme_brand",
                        static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From("branded:" + args[0].AsString()),
                        1,
                        1)),
                loggerFactory))
            {
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(Key, ct);

                beforeFingerprint = ((KahunaManager)node.Kahuna).UserFunctionFingerprint;

                KeyValueTransactionResult written = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"BEGIN SET `{Key}` acme_brand('value') COMMIT END"), null, null);

                Assert.True(written.Type == KeyValueResponseType.Set, $"{written.Type}: {written.Reason}");

                await node.Kahuna.FlushPersistenceAsync();
            }

            // The restarted node registers nothing. Its table holds only the built-ins.
            await using (EmbeddedKahunaNode node = new(
                PersistentOptions(storagePath, walPath, static _ => { }),
                loggerFactory))
            {
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(Key, ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                // The registry really is different, so the read below is not passing by accident.
                Assert.NotEqual(beforeFingerprint, kahuna.UserFunctionFingerprint);
                Assert.Empty(kahuna.GetUserFunctionStats());

                KeyValueTransactionResult read = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"GET `{Key}`"), null, null);

                Assert.True(read.Type == KeyValueResponseType.Get, $"{read.Type}: {read.Reason}");
                Assert.Equal("branded:value", Encoding.UTF8.GetString(read.Value ?? []));

                // And calling the retired function is a clean, deterministic error rather than a crash.
                KeyValueTransactionResult refused = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"BEGIN SET `{Key}` acme_brand('again') COMMIT END"), null, null);

                Assert.Equal(KeyValueResponseType.Errored, refused.Type);
                Assert.Contains("acme_brand", refused.Reason ?? "", StringComparison.Ordinal);
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }
}

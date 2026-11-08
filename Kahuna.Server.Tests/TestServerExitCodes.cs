using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

using Kahuna.Server;

namespace Kahuna.Server.Tests;

/// <summary>
/// The exit code of the server process, the only thing a supervisor reads. systemd restarts a unit with
/// <c>Restart=on-failure</c> only on a non-zero exit, and a Kubernetes Job counts a zero exit as success, so a
/// server that cannot start must exit non-zero, and one that is asked to stop must exit zero.
/// <para>
/// These run the real <c>Kahuna.Server.dll</c> from this test's output as a child process: the exit code is
/// decided by <c>Program</c> and the runtime, which no in-process host exercises. A configuration error must
/// also not escape <c>Main</c>, because the runtime then aborts with exit code 134 and, in the Docker images,
/// writes a crash dump.
/// </para>
/// </summary>
public sealed class TestServerExitCodes : IDisposable
{
    private static readonly TimeSpan ExitTimeout = TimeSpan.FromSeconds(90);

    private readonly string tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_exit_codes_" + Guid.NewGuid().ToString("N"));

    public TestServerExitCodes() => Directory.CreateDirectory(tempRoot);

    public void Dispose()
    {
        try { Directory.Delete(tempRoot, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public async Task ACommandLineThatDoesNotParse_ExitsWithTheUsageCode()
    {
        ServerRun run = await RunToExitAsync("bad-flag", ["--no-such-flag"]);

        Assert.Equal(ServerStartFailure.InvalidCommandLineExitCode, run.ExitCode);
        Assert.DoesNotContain("Unhandled exception", run.Output, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--help")]
    [InlineData("--version")]
    public async Task TheHelpAndTheVersion_ExitWithZero(string flag)
    {
        ServerRun run = await RunToExitAsync("info", [flag]);

        Assert.Equal(0, run.ExitCode);
    }

    [Fact]
    public async Task AConfigurationErrorFoundBeforeTheHostStarts_ExitsWithOne()
    {
        ServerRun run = await RunToExitAsync("unknown-target", [.. NodeArguments("unknown-target", clustered: false),
            "--pitr-backup-dir", Dir("unknown-target", "backups"),
            "--pitr-backup-target", "no-such-target"]);

        Assert.Equal(ServerStartFailure.ExitCode, run.ExitCode);
        Assert.Contains("Invalid backup target: 'no-such-target'", run.Output, StringComparison.Ordinal);
        Assert.Contains("Kahuna stopped because of an error", run.Output, StringComparison.Ordinal);
        Assert.DoesNotContain("Unhandled exception", run.Output, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ABackupStorageThatCannotStart_ExitsWithOne(bool clustered)
    {
        string name = clustered ? "start-failure-cluster" : "start-failure-standalone";

        ServerRun run = await RunToExitAsync(name, [.. NodeArguments(name, clustered),
            "--extension-assembly", FixturePath(),
            "--pitr-backup-dir", Dir(name, "backups"),
            "--pitr-backup-target", "test-remote",
            "--pitr-backup-scratch-dir", Dir(name, "scratch"),
            "--extension-option", $"test-remote.root={Dir(name, "remote")}", "test-remote.fail-start=true"]);

        Assert.Equal(ServerStartFailure.ExitCode, run.ExitCode);
        Assert.Contains("The test-remote backup target refused to start, as configured.", run.Output, StringComparison.Ordinal);
        Assert.Contains("Kahuna stopped because the node could not start", run.Output, StringComparison.Ordinal);
        Assert.DoesNotContain("Unhandled exception", run.Output, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AStopRequest_ExitsWithZero(bool clustered)
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "The stop request is a SIGTERM, which Windows does not have.");

        string name = clustered ? "stop-cluster" : "stop-standalone";
        int httpPort = FreePort();

        using ServerProcess server = ServerProcess.Start(Dir(name), [.. NodeArguments(name, clustered, httpPort)]);

        await server.WaitUntilListeningAsync(httpPort, ExitTimeout);
        await server.SendStopRequestAsync();

        Assert.True(await server.WaitForExitAsync(ExitTimeout), $"The server still runs {ExitTimeout} after SIGTERM.\n{server.Output}");
        Assert.Equal(0, server.ExitCode);
        Assert.DoesNotContain("Kahuna stopped because", server.Output, StringComparison.Ordinal);
    }

    private async Task<ServerRun> RunToExitAsync(string name, IReadOnlyList<string> arguments)
    {
        using ServerProcess server = ServerProcess.Start(Dir(name), arguments);

        Assert.True(await server.WaitForExitAsync(ExitTimeout), $"The server still runs {ExitTimeout} after its start.\n{server.Output}");
        return new ServerRun(server.ExitCode, server.Output);
    }

    /// <summary>
    /// A node on RocksDB with free ports. A clustered node lists peers that do not exist, so it never forms
    /// a cluster: a node that ignored a start failure would wait to join and never exit.
    /// </summary>
    private List<string> NodeArguments(string name, bool clustered, int? httpPort = null)
    {
        int grpcPort = FreePort();

        List<string> args =
        [
            "--raft-nodename", name,
            "--raft-host", "localhost",
            "--http-ports", (httpPort ?? FreePort()).ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--grpc-cleartext-ports", grpcPort.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--allow-plaintext-listener",
            "--storage", "rocksdb",
            "--storage-path", Dir(name, "data"),
            "--wal-storage", "rocksdb",
            "--wal-path", Dir(name, "wal")
        ];

        if (clustered)
        {
            args.AddRange(
            [
                "--raft-port", grpcPort.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "--raft-grpc-scheme", "http://",
                "--initial-cluster",
                $"localhost:{FreePort()}",
                $"localhost:{FreePort()}"
            ]);
        }

        return args;
    }

    private string Dir(string name, string? sub = null)
    {
        string path = sub is null ? Path.Combine(tempRoot, name) : Path.Combine(tempRoot, name, sub);
        Directory.CreateDirectory(path);
        return path;
    }

    /// <summary>Where the storage fixture assembly is built, next to this assembly's output.</summary>
    private static string FixturePath()
    {
        string baseDirectory = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);

        string path = baseDirectory.Replace(
            Path.DirectorySeparatorChar + "Kahuna.Server.Tests" + Path.DirectorySeparatorChar,
            Path.DirectorySeparatorChar + "Kahuna.TestStorageExtension" + Path.DirectorySeparatorChar,
            StringComparison.Ordinal);

        return Path.Combine(path, "Kahuna.TestStorageExtension.dll");
    }

    private static int FreePort()
    {
        using TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private sealed record ServerRun(int ExitCode, string Output);

    /// <summary>One server process started from this test's output. Dispose kills only this process.</summary>
    private sealed class ServerProcess : IDisposable
    {
        private readonly Process process;

        private readonly StringBuilder output = new();

        private readonly Lock outputSync = new();

        private readonly TaskCompletionSource exited = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private ServerProcess(Process process) => this.process = process;

        public int ExitCode => process.ExitCode;

        public string Output
        {
            get
            {
                lock (outputSync)
                    return output.ToString();
            }
        }

        public static ServerProcess Start(string workingDirectory, IReadOnlyList<string> arguments)
        {
            ProcessStartInfo info = new("dotnet")
            {
                WorkingDirectory = workingDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };

            info.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "Kahuna.Server.dll"));
            foreach (string argument in arguments)
                info.ArgumentList.Add(argument);

            // A crash dump would hide a regression behind a slow test; the exit code is the assertion.
            info.Environment["DOTNET_DbgEnableMiniDump"] = "0";

            Process process = new() { StartInfo = info, EnableRaisingEvents = true };
            ServerProcess server = new(process);

            process.OutputDataReceived += (_, e) => server.Append(e.Data);
            process.ErrorDataReceived += (_, e) => server.Append(e.Data);
            process.Exited += (_, _) => server.exited.TrySetResult();

            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            return server;
        }

        public async Task<bool> WaitForExitAsync(TimeSpan timeout)
        {
            Task finished = await Task.WhenAny(exited.Task, Task.Delay(timeout, TestContext.Current.CancellationToken));
            if (finished != exited.Task)
                return false;

            // Exited can fire before the redirected streams are drained; this waits for both.
            await process.WaitForExitAsync(TestContext.Current.CancellationToken);
            return true;
        }

        /// <summary>Waits until the node answers HTTP, so the stop request reaches a started host.</summary>
        public async Task WaitUntilListeningAsync(int httpPort, TimeSpan timeout)
        {
            using HttpClient http = new() { Timeout = TimeSpan.FromSeconds(5) };
            DateTime deadline = DateTime.UtcNow + timeout;

            while (DateTime.UtcNow < deadline)
            {
                if (exited.Task.IsCompleted)
                    throw new InvalidOperationException($"The server exited with code {process.ExitCode} before it listened.\n{Output}");

                try
                {
                    using HttpResponseMessage response =
                        await http.GetAsync($"http://localhost:{httpPort}/v1/cluster/health", TestContext.Current.CancellationToken);
                    return;
                }
                catch (HttpRequestException)
                {
                    // Not listening yet.
                }
                catch (TaskCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
                {
                    // The request timed out; try again.
                }

                await Task.Delay(TimeSpan.FromMilliseconds(250), TestContext.Current.CancellationToken);
            }

            throw new TimeoutException($"The server did not listen within {timeout}.\n{Output}");
        }

        /// <summary>Sends SIGTERM, the stop request of systemd, Docker and Kubernetes, to this process only.</summary>
        public async Task SendStopRequestAsync()
        {
            using Process kill = Process.Start(new ProcessStartInfo("kill")
            {
                ArgumentList = { "-TERM", process.Id.ToString(System.Globalization.CultureInfo.InvariantCulture) },
                UseShellExecute = false
            })!;

            await kill.WaitForExitAsync(TestContext.Current.CancellationToken);
            Assert.Equal(0, kill.ExitCode);
        }

        public void Dispose()
        {
            if (!exited.Task.IsCompleted)
            {
                try
                {
                    process.Kill(entireProcessTree: true);
                    process.WaitForExit(TimeSpan.FromSeconds(30));
                }
                catch (InvalidOperationException)
                {
                    // It exited between the check and the kill.
                }
            }

            process.Dispose();
        }

        private void Append(string? line)
        {
            if (line is null)
                return;

            lock (outputSync)
                output.AppendLine(line);
        }
    }
}

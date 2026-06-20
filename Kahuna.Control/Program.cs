
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;
using Kahuna.Client;
using Kahuna.Control;
using Kahuna.Control.Commands;
using Spectre.Console;

ParserResult<KahunaControlOptions> optsResult = Parser.Default.ParseArguments<KahunaControlOptions>(args);

KahunaControlOptions? opts = optsResult.Value;
if (opts is null)
    return;

KahunaClient connection = await GetConnection(opts);

if (IsSingleCommand(opts))
{
    try
    {
        string? format = opts.Format;

        if (!string.IsNullOrEmpty(opts.Set))
        {
            await KeyValueSetCommand.Execute(connection, opts.Set, opts.Value, opts.Expires, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.Get))
        {
            await KeyValueGetCommand.Execute(connection, opts.Get, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.GetByBucket))
        {
            await KeyValueGetByBucketCommand.Execute(connection, opts.GetByBucket, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.ScanByPrefix))
        {
            await KeyValueScanByPrefixCommand.Execute(connection, opts.ScanByPrefix, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.Lock))
        {
            await LockCommand.Execute(connection, opts.Lock, opts.Expires, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.ExtendLock))
        {
            await ExtendLockCommand.Execute(connection, opts.ExtendLock, opts.Owner!, opts.Expires, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.Unlock))
        {
            await UnlockCommand.Execute(connection, opts.Unlock, opts.Owner!, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.CreateSequence))
        {
            await SequenceCommand.Create(connection, opts.CreateSequence, opts.InitialValue, opts.Increment, opts.MaxValue, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.GetSequence))
        {
            await SequenceCommand.Get(connection, opts.GetSequence, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.NextSequence))
        {
            await SequenceCommand.Next(connection, opts.NextSequence, opts.IdempotencyKey, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.ReserveSequence))
        {
            await SequenceCommand.Reserve(connection, opts.ReserveSequence, opts.Count, opts.IdempotencyKey, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.DeleteSequence))
        {
            await SequenceCommand.Delete(connection, opts.DeleteSequence, format);
            return;
        }

        if (opts.ClusterMembers)
        {
            await ClusterMembersCommand.Execute(connection, format);
            return;
        }

        if (opts.BackupFull)
        {
            await BackupFullCommand.Execute(connection, format);
            return;
        }

        if (opts.BackupIncremental)
        {
            if (opts.ParentBackupId is null)
            {
                AnsiConsole.MarkupLine("[red]--backup-incremental requires --parent-backup-id[/]");
                return;
            }
            await BackupIncrementalCommand.Execute(connection, opts.ParentBackupId.Value, format);
            return;
        }

        if (opts.BackupCoordinated)
        {
            await BackupCoordinatedCommand.Execute(connection, format);
            return;
        }

        if (opts.ListBackups)
        {
            await ListBackupsCommand.Execute(connection, format);
            return;
        }

        if (opts.BackupChain.HasValue)
        {
            await BackupChainCommand.Execute(connection, opts.BackupChain.Value, format);
            return;
        }

        if (opts.Restore.HasValue)
        {
            if (string.IsNullOrWhiteSpace(opts.TargetDir))
            {
                AnsiConsole.MarkupLine("[red]--restore requires --target-dir[/]");
                return;
            }
            await RestoreCommand.Execute(connection, opts.Restore.Value, opts.TargetDir, opts.TargetTimeMs, format);
            return;
        }
    }
    catch (Exception ex)
    {
        AnsiConsole.WriteLine("{0}: {1}", ex.GetType().Name, ex.Message);
        return;
    }
}

await InteractiveConsole.Run(connection, opts);
return;

/// <summary>
/// Starts a connection to the Kahuna cluster or load balancer
/// </summary>
static Task<KahunaClient> GetConnection(KahunaControlOptions opts)
{
    string? connectionString = opts.ConnectionSource;

    string[] connectionPool;

    if (string.IsNullOrEmpty(connectionString))
        connectionPool = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];
    else
        connectionPool = connectionString.Split(",", StringSplitOptions.RemoveEmptyEntries).ToArray();

    bool insecure = opts.Insecure || connectionPool.All(IsLocalhost);

    KahunaOptions kahunaOptions = new() { AllowInsecureCertificateValidation = insecure };

    return Task.FromResult(new KahunaClient(connectionPool, null, null, kahunaOptions));
}

static bool IsLocalhost(string url)
{
    if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri))
        return false;
    string host = uri.Host;
    return host is "localhost" or "127.0.0.1" or "::1" or "[::1]";
}

static bool IsSingleCommand(KahunaControlOptions kahunaControlOptions)
{
    if (!string.IsNullOrEmpty(kahunaControlOptions.Set))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.Get))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.GetByBucket))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.ScanByPrefix))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.Lock))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.Unlock))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.ExtendLock))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.CreateSequence))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.GetSequence))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.NextSequence))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.ReserveSequence))
        return true;

    if (!string.IsNullOrEmpty(kahunaControlOptions.DeleteSequence))
        return true;

    if (kahunaControlOptions.ClusterMembers)
        return true;

    if (kahunaControlOptions.BackupFull)
        return true;

    if (kahunaControlOptions.BackupIncremental)
        return true;

    if (kahunaControlOptions.BackupCoordinated)
        return true;

    if (kahunaControlOptions.ListBackups)
        return true;

    if (kahunaControlOptions.BackupChain.HasValue)
        return true;

    if (kahunaControlOptions.Restore.HasValue)
        return true;

    return false;
}

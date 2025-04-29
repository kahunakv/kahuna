
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

        if (!string.IsNullOrEmpty(opts.GetByPrefix))
        {
            await KeyValueGetByPrefixCommand.Execute(connection, opts.GetByPrefix, format);
            return;
        }

        if (!string.IsNullOrEmpty(opts.ScanByPrefix))
        {
            await KeyValueGetByPrefixCommand.Execute(connection, opts.ScanByPrefix, format);
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

    return Task.FromResult(new KahunaClient(connectionPool, null));
}

static bool IsSingleCommand(KahunaControlOptions kahunaControlOptions)
{
    if (!string.IsNullOrEmpty(kahunaControlOptions.Set))
        return true;
    
    if (!string.IsNullOrEmpty(kahunaControlOptions.Get))
        return true;
    
    if (!string.IsNullOrEmpty(kahunaControlOptions.GetByPrefix))
        return true;
    
    if (!string.IsNullOrEmpty(kahunaControlOptions.ScanByPrefix))
        return true;
    
    if (!string.IsNullOrEmpty(kahunaControlOptions.Lock))
        return true;
    
    if (!string.IsNullOrEmpty(kahunaControlOptions.Unlock))
        return true;
    
    if (!string.IsNullOrEmpty(kahunaControlOptions.ExtendLock))
        return true;

    return false;
}
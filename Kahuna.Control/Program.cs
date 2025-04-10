
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

ParserResult<Options> optsResult = Parser.Default.ParseArguments<Options>(args);

Options? opts = optsResult.Value;
if (opts is null)
    return;

KahunaClient connection = await GetConnection(opts);

if (IsSingleCommand(opts))
{
    string? format = opts.Format;
    
    if (!string.IsNullOrEmpty(opts.Set))
    {
        await KeyValueSetCommand.Execute(connection, opts.Set, opts.Value, format);
        return;
    }
    
    if (!string.IsNullOrEmpty(opts.Get))
    {
        await KeyValueGetCommand.Execute(connection, opts.Get, format);
        return;
    }
    
    if (!string.IsNullOrEmpty(opts.Lock))
    {
        await LockCommand.Execute(connection, opts.Lock, format);
        return;
    }
}

await InteractiveConsole.Run(connection);
return;

static Task<KahunaClient> GetConnection(Options opts)
{
    string? connectionString = opts.ConnectionSource;

    string[] connectionPool;

    if (string.IsNullOrEmpty(connectionString))
        connectionPool = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];
    else
        connectionPool = connectionString.Split(",", StringSplitOptions.RemoveEmptyEntries).ToArray();

    return Task.FromResult(new KahunaClient(connectionPool, null, new Kahuna.Client.Communication.GrpcCommunication(null)));
}

static bool IsSingleCommand(Options options)
{
    if (!string.IsNullOrEmpty(options.Set))
        return true;
    
    if (!string.IsNullOrEmpty(options.Get))
        return true;
    
    if (!string.IsNullOrEmpty(options.Lock))
        return true;
    
    if (!string.IsNullOrEmpty(options.ExtendLock))
        return true;

    return false;
}

/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Text.Json;
using DotNext.Threading;
using DotNext.Threading.Tasks;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using RadLine;
using Spectre.Console;

namespace Kahuna.Control;

/// <summary>
/// The InteractiveConsole class provides an entry point for running an interactive shell
/// that facilitates communication with the Kahuna cluster or load balancer.
/// This console provides a command-line interface supporting various interactive operations
/// and utilizes session history and advanced input handling for user convenience.
/// </summary>
public static class InteractiveConsole
{
    public static async Task Run(KahunaClient connection)
    {
        Assembly assembly = Assembly.GetExecutingAssembly();
        FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
        
        AnsiConsole.MarkupLine("[green]Kahuna Shell {0} (alpha)[/]\n", fvi.FileMajorPart + "." + fvi.FileMinorPart + "." + fvi.FileBuildPart);

        string historyPath = string.Concat(Path.GetTempPath(), Path.PathSeparator, "kahuna.history.json");
        List<string> history = await GetHistory(historyPath);

        LineEditor? editor = null;
        Dictionary<string, KahunaLock> locks = new();
        Dictionary<string, KahunaLock> elocks = new();
        Dictionary<string, KahunaTransactionScript> scripts = new();

        if (LineEditor.IsSupported(AnsiConsole.Console))
        {
            string[] keywords =
            [
                // ephemeral key/values
                "eset",
                "eget",
                "edel",
                "edelete",
                "eextend",
                "eexists",
                // key/values
                "set",
                "get",
                "del",
                "delete",
                "extend",
                "exists",
                "nx",
                "xx",
                "ex",
                "at",
                "cmp",
                "cmprev",
                "by",
                "prefix",
                // control structures or operators
                "not",
                "let",
                "if",
                "else",
                "then",
                "end",
                "for",
                "in",
                "do",
                "begin",
                "rollback",
                "commit",
                "return",
                "sleep",
                "throw",
                "found",
                // locks
                "lock",
                "extend-lock",
                "unlock",
                "get-lock",
                "elock",
                "eextend-lock",
                "eunlock",
                "eget-lock",
            ];

            string[] functions = [
                "to_int",
                "to_long",
                "to_double",
                "to_float",
                "to_boolean",
                "to_bool",
                "to_string",
                "to_str",
                "is_int",
                "is_integer",
                "is_long",
                "is_double",
                "is_float",
                "is_boolean",
                "is_bool",
                "is_string",
                "is_str",
                "is_null",
                "to_json",
                "revision",
                "rev",
                "length",
                "expires",
                "len",
                "length",
                "count",
                "upper",
                "lower",
                "concat",
                "trim",
                "max",
                "min",
                "round",
                "ceil",
                "floor",
                "abs",
                "pow",
                "current_time",                
            ];

            string[] commands =
            [
                "run",
                "clear",
                "exit",
                "quit"
            ];
            
            string[] constants =
            [
                "null",
                "true",
                "false"
            ];
            
            string[] regexes =
            [
                @"(?<number>\b\d+(\.\d+)?\b)",
                @"(?<singlequote>'(?:\\'|[^'])*')",
                @"(?<escapedquote>`(?:\\`|[^`])*`)",
                "(?<doublequote>\"(?:\\\\\"|[^\"])*\")"
            ];

            WordHighlighter worldHighlighter = new();

            Style funcStyle = new(foreground: Color.Aqua);
            Style keywordStyle = new(foreground: Color.Blue);
            Style commandStyle = new(foreground: Color.LightSkyBlue1);
            Style constantsStyle = new(foreground: Color.LightPink3);

            foreach (string keyword in keywords)
                worldHighlighter.AddWord(keyword, keywordStyle);

            foreach (string func in functions)
                worldHighlighter.AddWord(func, funcStyle);

            foreach (string command in commands)
                worldHighlighter.AddWord(command, commandStyle);
            
            foreach (string constant in constants)
                worldHighlighter.AddWord(constant, constantsStyle);
            
            foreach (string regex in regexes)
                worldHighlighter.AddRegex(regex, constantsStyle);

            editor = new()
            {
                MultiLine = true,
                Text = "",
                Prompt = new MyLineNumberPrompt(new(foreground: Color.PaleTurquoise1)),
                //Completion = new TestCompletion(),        
                Highlighter = worldHighlighter
            };

            foreach (string item in history)
                editor.History.Add(item);
        }

        Console.CancelKeyPress += delegate
        {
            AnsiConsole.MarkupLine("[cyan]\nExiting...[/]");

            foreach (KeyValuePair<string, KahunaLock> kvp in locks)
            {
                AnsiConsole.MarkupLine("[yellow]Disposing lock {0}...[/]", Encoding.UTF8.GetString(kvp.Value.Owner));

                kvp.Value.DisposeAsync().Wait();
            }
            
            SaveHistory(historyPath, history).Wait();
        };

        while (true)
        {
            try
            {
                string? command;

                if (editor is not null)
                    command = await editor.ReadLine(CancellationToken.None);
                else
                    command = AnsiConsole.Prompt(new TextPrompt<string>("kahuna-cli> ").AllowEmpty());

                if (string.IsNullOrWhiteSpace(command))
                    continue;

                string commandTrim = command.Trim();
                
                if (string.Equals(commandTrim, "exit", StringComparison.InvariantCultureIgnoreCase) || string.Equals(commandTrim, "quit", StringComparison.InvariantCultureIgnoreCase))
                {
                    await Exit(historyPath, history, locks, elocks);
                    break;
                }
                
                if (string.Equals(commandTrim, "clear", StringComparison.InvariantCultureIgnoreCase))
                {
                    AnsiConsole.Clear();
                    continue;
                }
                
                if (commandTrim.StartsWith("run ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await LoadAndRunScript(connection, scripts, history, commandTrim);
                    continue;
                }
                
                if (commandTrim.StartsWith("lock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await TryLock(connection, history, commandTrim, locks, LockDurability.Persistent);
                    continue;
                }
                
                if (commandTrim.StartsWith("elock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await TryLock(connection, history, commandTrim, elocks, LockDurability.Ephemeral);
                    continue;
                }
                
                if (commandTrim.StartsWith("unlock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await TryUnlock(history, commandTrim, locks);
                    continue;
                }

                if (commandTrim.StartsWith("eunlock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await TryUnlock(history, commandTrim, elocks);
                    continue;
                }

                if (commandTrim.StartsWith("get-lock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await GetLock(history, commandTrim, locks);
                    continue;
                }

                if (commandTrim.StartsWith("eget-lock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await GetLock(history, commandTrim, elocks);
                    continue;
                }
                
                if (commandTrim.StartsWith("extend-lock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await TryExtendLock(history, commandTrim, locks);
                    continue;
                }

                if (commandTrim.StartsWith("eextend-lock ", StringComparison.InvariantCultureIgnoreCase))
                {
                    await TryExtendLock(history, commandTrim, elocks);
                    continue;
                }

                await RunCommand(connection, scripts, history, commandTrim);
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
            }
        }
    }

    /// <summary>
    /// Cleans up resources including disposing locks and saving the command history.
    /// This method ensures proper disposal of persistent and ephemeral locks, handling any exceptions that occur,
    /// as well as saving the session's history to a specified path.
    /// </summary>
    /// <param name="historyPath">The file path where the session history should be saved.</param>
    /// <param name="history">A list of strings representing the command history of the session.</param>
    /// <param name="locks">A dictionary of persistent locks to be disposed, keyed by their identifiers.</param>
    /// <param name="elocks">A dictionary of ephemeral locks to be disposed, keyed by their identifiers.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task Exit(string historyPath, List<string> history, Dictionary<string, KahunaLock> locks, Dictionary<string, KahunaLock> elocks)
    {
        foreach (KeyValuePair<string, KahunaLock> kvp in locks)
        {
            try
            {
                AnsiConsole.MarkupLine("[yellow]Disposing persistent lock {0}...[/]", Markup.Escape(Encoding.UTF8.GetString(kvp.Value.Owner)));

                await kvp.Value.DisposeAsync();
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
            }
        }
        
        foreach (KeyValuePair<string, KahunaLock> kvp in elocks)
        {
            try
            {
                AnsiConsole.MarkupLine("[yellow]Disposing ephemeral lock {0}...[/]", Markup.Escape(Encoding.UTF8.GetString(kvp.Value.Owner)));

                await kvp.Value.DisposeAsync();
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
            }
        }

        await SaveHistory(historyPath, history);
    }

    /// <summary>
    /// Retrieves the command history from the specified file path.
    /// If the file exists and contains valid data, it reads and deserializes the command history.
    /// If the file does not exist or contains invalid data, an empty list is returned.
    /// </summary>
    /// <param name="historyPath">The file path where the command history is stored.</param>
    /// <returns>A task representing the asynchronous operation, containing a list of strings representing the command history.</returns>
    private static async Task<List<string>> GetHistory(string historyPath)
    {
        List<string>? history = [];

        if (File.Exists(historyPath))
        {
            try
            {
                string historyText = await File.ReadAllTextAsync(historyPath);
                history = JsonSerializer.Deserialize<List<string>>(historyText);
            }
            catch
            {
                AnsiConsole.MarkupLine("[yellow]Found invalid history[/]");
            }
        }

        history ??= [];

        return history;
    }

    /// <summary>
    /// Saves the command history to a specified file path in JSON format.
    /// This method ensures that the session history is serialized and stored persistently for future use.
    /// </summary>
    /// <param name="historyPath">The path to the file where the session history will be saved.</param>
    /// <param name="history">A list of strings representing the session's command history. If null, no action is taken.</param>
    /// <returns>A task representing the asynchronous save operation.</returns>
    private static async Task SaveHistory(string historyPath, List<string>? history)
    {
        if (history is not null)
            await File.WriteAllTextAsync(historyPath, JsonSerializer.Serialize(history));
        
        //AnsiConsole.MarkupLine("[cyan]Saving history to {0}...[/]", Markup.Escape(historyPath));
    }

    /// <summary>
    /// Attempts to acquire a lock on the specified resource through the provided connection,
    /// updates the history, and stores the lock in the provided dictionary if successful.
    /// Displays the acquisition result in the console output.
    /// </summary>
    /// <param name="connection">The KahunaClient instance responsible for managing communication and lock operations.</param>
    /// <param name="history">A list of strings to record the commands executed during the session.</param>
    /// <param name="commandTrim">The command string containing lock information, trimmed of whitespace.</param>
    /// <param name="locks">A dictionary for storing successfully acquired locks, keyed by resource names.</param>
    /// <param name="durability">The durability level of the lock, defining its persistence mode (ephemeral or persistent).</param>
    /// <returns>A task representing the asynchronous operation of obtaining the lock.</returns>
    private static async Task TryLock(KahunaClient connection, List<string> history, string commandTrim, Dictionary<string, KahunaLock> locks, LockDurability durability)
    {
        history.Add(commandTrim);

        string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);

        KahunaLock kahunaLock = await connection.GetOrCreateLock(parts[1], int.Parse(parts[2]), durability);

        if (kahunaLock.IsAcquired)
        {
            AnsiConsole.MarkupLine("[cyan]f{0} acquired {1}[/]\n", Markup.Escape(kahunaLock.FencingToken.ToString()), Markup.Escape(Encoding.UTF8.GetString(kahunaLock.Owner)));
            
            locks.TryAdd(parts[1], kahunaLock);
        }
        else
            AnsiConsole.MarkupLine("[yellow]not acquired[/]\n");
    }

    /// <summary>
    /// Attempts to unlock a specified resource by disposing the corresponding lock object if it exists.
    /// Adds the command for unlocking to the session history and notifies the user about the result of the operation.
    /// </summary>
    /// <param name="history">A list of strings that represents the session command history.</param>
    /// <param name="commandTrim">The processed command string indicating the unlock operation, including the target resource.</param>
    /// <param name="locks">A dictionary mapping resource identifiers to their associated KahunaLock instances, representing acquired locks.</param>
    /// <returns>A task representing the asynchronous operation of unlocking a resource.</returns>
    private static async Task TryUnlock(List<string> history, string commandTrim, Dictionary<string, KahunaLock> locks)
    {
        history.Add(commandTrim);
                    
        string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);

        if (locks.TryGetValue(parts[1], out KahunaLock? kahunaLock))
        {
            await kahunaLock.DisposeAsync();

            //if (success)
                AnsiConsole.MarkupLine("[cyan]unlocked[/]\n");
            //else
            //    AnsiConsole.MarkupLine("[yellow]not unlocked[/]");
            
            locks.Remove(parts[1]);
        } 
        else
        {
            AnsiConsole.MarkupLine("[yellow]not acquired[/]\n");
        }
    }

    /// <summary>
    /// Attempts to extend the specified lock within the given duration.
    /// This method parses the command, updates the session history, and extends the lock
    /// if it is found in the provided dictionary of locks. The result of the extension
    /// operation is displayed in the console.
    /// </summary>
    /// <param name="history">A list of strings representing the session's command history.</param>
    /// <param name="commandTrim">The parsed and trimmed command to execute, containing lock information and duration.</param>
    /// <param name="locks">A dictionary of locks to search, keyed by their identifiers.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task TryExtendLock(List<string> history, string commandTrim, Dictionary<string, KahunaLock> locks)
    {
        history.Add(commandTrim);
                    
        string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);

        if (locks.TryGetValue(parts[1], out KahunaLock? kahunaLock))
        {
            (bool success, long fencingToken) = await kahunaLock.TryExtend(int.Parse(parts[2]));

            if (success)
                AnsiConsole.MarkupLine("[cyan]f{0} extended {1}[/]\n", fencingToken, Markup.Escape(Encoding.UTF8.GetString(kahunaLock.Owner)));
            else
                AnsiConsole.MarkupLine("[yellow]not acquired[/]\n");
        }
        else
        {
            AnsiConsole.MarkupLine("[yellow]not acquired[/]\n");
        }
    }

    /// <summary>
    /// Processes a locking command by inspecting the provided lock dictionary and printing information
    /// about the lock's status or existence. If the specified lock is found, its information is retrieved
    /// and displayed; otherwise, a "not found" message is shown.
    /// </summary>
    /// <param name="history">A list that maintains a history of executed commands.</param>
    /// <param name="commandTrim">The trimmed command string used to identify the lock.</param>
    /// <param name="locks">A dictionary containing locks associated with their identifiers.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task GetLock(List<string> history, string commandTrim, Dictionary<string, KahunaLock> locks)
    {
        history.Add(commandTrim);
                    
        string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);

        if (locks.TryGetValue(parts[1], out KahunaLock? kahunaLock))
        {
            KahunaLockInfo? info = await kahunaLock.GetInfo();

            if (info is not null)
                AnsiConsole.MarkupLine("[cyan]f{0} {1}[/]\n", info.FencingToken, Markup.Escape(Encoding.UTF8.GetString(info.Owner ?? [])));
            else
                AnsiConsole.MarkupLine("[yellow]not found[/]\n");
        }
        else
        {
            AnsiConsole.MarkupLine("[yellow]not found[/]\n");
        }
    }

    /// <summary>
    /// Executes a specified command using the provided connection and scripts, processes the result,
    /// and logs the command to history if it completes without errors or retries.
    /// </summary>
    /// <param name="connection">The Kahuna client connection used to execute the command.</param>
    /// <param name="scripts">A dictionary containing available transaction scripts keyed by their name.</param>
    /// <param name="history">A list storing the history of successfully executed commands.</param>
    /// <param name="commandTrim">The input command to be executed, trimmed of any extraneous whitespace.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task RunCommand(KahunaClient connection, Dictionary<string, KahunaTransactionScript> scripts, List<string> history, string commandTrim)
    {
        //ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (!scripts.TryGetValue(commandTrim, out KahunaTransactionScript? script))
        {
            script = connection.LoadTransactionScript(commandTrim);
            scripts.Add(commandTrim, script);
        }
        
        KahunaKeyValueTransactionResult result = await script.Run();
                
        switch (result.Type)
        {
            case KeyValueResponseType.Get:
                if (result.Values is not null)
                {
                    if (result.Values.Count == 1)
                    {
                        AnsiConsole.MarkupLine(
                            "r{0} [cyan]{1}[/] {2}ms\n",
                            result.Values[0].Revision,                                                         
                            Markup.Escape(GetHumanValue(result.Values[0])), 
                            result.TimeElapsedMs
                        );
                    }
                    else
                    {
                        foreach (KahunaKeyValueTransactionResultValue value in result.Values)
                        {
                            if (value.Key is null)
                                AnsiConsole.MarkupLine(
                                    "r{0} [cyan]{1}[/]",
                                    value.Revision >= 0 ? value.Revision : "-",                                   
                                    Markup.Escape(GetHumanValue(value))
                                );
                            else
                            {
                                AnsiConsole.MarkupLine(
                                    "r{0} [lightpink3]{1}[/] [cyan]{2}[/]",
                                    value.Revision >= 0 ? value.Revision : "-",
                                    Markup.Escape(value.Key ?? ""),
                                    Markup.Escape(GetHumanValue(value)) 
                                );
                            }
                        }

                        AnsiConsole.WriteLine("{0}ms\n", result.TimeElapsedMs);
                    }                    
                }

                break;
            
            case KeyValueResponseType.DoesNotExist:
                if (result.Values is not null)
                {
                    foreach (KahunaKeyValueTransactionResultValue value in result.Values)
                        AnsiConsole.MarkupLine("r{0} [yellow]not found[/] {1}ms\n", value.Revision, result.TimeElapsedMs);
                }

                break;
            
            case KeyValueResponseType.Set:
                if (result.Values is not null)
                {
                    foreach (KahunaKeyValueTransactionResultValue value in result.Values)
                        AnsiConsole.MarkupLine(
                            "r{0} [cyan]set[/] {1}ms\n", 
                            value.Revision >= 0 ? value.Revision : "-",
                            result.TimeElapsedMs
                        );
                }
                else
                {
                    AnsiConsole.MarkupLine(
                        "r{0} [cyan]set[/] {1}ms\n", 
                        "-",
                        result.TimeElapsedMs
                    );
                }

                break;
            
            case KeyValueResponseType.NotSet:
                if (result.Values is not null)
                {
                    foreach (KahunaKeyValueTransactionResultValue value in result.Values)
                        AnsiConsole.MarkupLine(
                            "r{0} [yellow]not set[/] {1}ms\n", 
                            value.Revision >= 0 ? value.Revision : "-",
                            result.TimeElapsedMs
                        );
                } 
                else                                 
                    AnsiConsole.MarkupLine("r{0} [yellow]not set[/] {1}ms\n", 0, result.TimeElapsedMs);
                
                break;
            
            case KeyValueResponseType.Deleted:
                if (result.Values is not null)
                {
                    foreach (KahunaKeyValueTransactionResultValue value in result.Values)
                        AnsiConsole.MarkupLine(
                            "r{0} [cyan]deleted[/] {1}ms\n", 
                            value.Revision >= 0 ? value.Revision : "-",
                            result.TimeElapsedMs
                        );
                } 
                else                                 
                    AnsiConsole.MarkupLine("r{0} [cyan]deleted[/] {1}ms\n", 0, result.TimeElapsedMs);           
                
                break;
            
            case KeyValueResponseType.Extended:
                
                if (result.Values is not null)
                {
                    foreach (KahunaKeyValueTransactionResultValue value in result.Values)
                        AnsiConsole.MarkupLine(
                            "r{0} [cyan]extended[/] {1}ms\n", 
                            value.Revision >= 0 ? value.Revision : "-",
                            result.TimeElapsedMs
                        );
                } 
                else                                 
                    AnsiConsole.MarkupLine("r{0} [yellow]extended[/] {1}ms\n", 0, result.TimeElapsedMs); 
                                
                break;
            
            case KeyValueResponseType.Exists:
                if (result.Values is not null)
                {
                    foreach (KahunaKeyValueTransactionResultValue value in result.Values)
                        AnsiConsole.MarkupLine(
                            "r{0} [cyan]exists[/] {1}ms\n", 
                            value.Revision >= 0 ? value.Revision : "-",
                            result.TimeElapsedMs
                        );
                } 
                else                                 
                    AnsiConsole.MarkupLine("r{0} [cyan]exists[/] {1}ms\n", 0, result.TimeElapsedMs); 
                                
                break;

            case KeyValueResponseType.Locked:
            case KeyValueResponseType.Unlocked:
            case KeyValueResponseType.Prepared:
            case KeyValueResponseType.Committed:
            case KeyValueResponseType.RolledBack:
            case KeyValueResponseType.Errored:
            case KeyValueResponseType.InvalidInput:
            case KeyValueResponseType.MustRetry:
            case KeyValueResponseType.Aborted:
            case KeyValueResponseType.AlreadyLocked:
            default:
                AnsiConsole.MarkupLine("[yellow]{0}[/]ms\n", result.Type, result.TimeElapsedMs);
                break;
        }
        
        if (result.Type != KeyValueResponseType.Errored && result.Type != KeyValueResponseType.MustRetry)
            history.Add(commandTrim);
    }

    /// <summary>
    /// Loads and executes a script file by reading its contents and running its commands.
    /// This method validates the existence of the script file, reads its contents asynchronously,
    /// and attempts to execute the script commands using the provided `KahunaClient` connection.
    /// </summary>
    /// <param name="connection">An instance of <see cref="KahunaClient"/> used to execute the script commands.</param>
    /// <param name="scripts">A dictionary of available transaction scripts, keyed by their identifiers.</param>
    /// <param name="history">A list of previous commands executed during the session.</param>
    /// <param name="commandTrim">The trimmed command string that contains the script file path.</param>
    /// <returns>A task representing the asynchronous execution of the script.</returns>
    private static async Task LoadAndRunScript(KahunaClient connection, Dictionary<string, KahunaTransactionScript> scripts, List<string> history, string commandTrim)
    {
        string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);
        
        if (!File.Exists(parts[1]))
        {
            AnsiConsole.MarkupLine("[red]File does not exist[/]");
            return;
        }
        
        string scriptText = await File.ReadAllTextAsync(parts[1]);
        if (string.IsNullOrWhiteSpace(scriptText))
            return;
        
        try
        {
            AnsiConsole.MarkupLine("[purple]{0}[/]", Markup.Escape(scriptText));

            await RunCommand(connection, scripts, history, scriptText);
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
        }
    }

    private static string GetHumanValue(KahunaKeyValueTransactionResultValue result)
    {
        if (result.Value is null) 
            return "(null)";

        string str = Encoding.UTF8.GetString(result.Value);
        if (string.IsNullOrEmpty(str)) 
            return "(empty str)";

        return str;
    }
}
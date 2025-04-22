
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Text.Json;
using DotNext.Threading.Tasks;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kommander.Diagnostics;
using RadLine;
using Spectre.Console;

namespace Kahuna.Control;

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
        Dictionary<string, KahunaScript> scripts = new();

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
                    foreach (KeyValuePair<string, KahunaLock> kvp in locks)
                    {
                        try
                        {
                            AnsiConsole.MarkupLine("[yellow]Disposing lock {0}...[/]", Markup.Escape(Encoding.UTF8.GetString(kvp.Value.Owner)));

                            await kvp.Value.DisposeAsync();
                        }
                        catch (Exception ex)
                        {
                            AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
                        }
                    }

                    await SaveHistory(historyPath, history);
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
                    history.Add(commandTrim);
                    
                    string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);

                    KahunaLock kahunaLock = await connection.GetOrCreateLock(parts[1], int.Parse(parts[2]));

                    if (kahunaLock.IsAcquired)
                    {
                        AnsiConsole.MarkupLine("[cyan]f{0} acquired {1}[/]\n", Markup.Escape(kahunaLock.FencingToken.ToString()), Markup.Escape(Encoding.UTF8.GetString(kahunaLock.Owner)));
                        
                        locks.TryAdd(parts[1], kahunaLock);
                    }
                    else
                        AnsiConsole.MarkupLine("[yellow]not acquired[/]\n");

                    continue;
                }
                
                if (commandTrim.StartsWith("unlock ", StringComparison.InvariantCultureIgnoreCase))
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
                    
                    continue;
                }
                
                if (commandTrim.StartsWith("get-lock ", StringComparison.InvariantCultureIgnoreCase))
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
                    
                    continue;
                }
                
                if (commandTrim.StartsWith("extend-lock ", StringComparison.InvariantCultureIgnoreCase))
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
    
    private static async Task SaveHistory(string historyPath, List<string>? history)
    {
        if (history is not null)
            await File.WriteAllTextAsync(historyPath, JsonSerializer.Serialize(history));
        
        //AnsiConsole.MarkupLine("[cyan]Saving history to {0}...[/]", Markup.Escape(historyPath));
    }

    private static async Task RunCommand(KahunaClient connection, Dictionary<string, KahunaScript> scripts, List<string> history, string commandTrim)
    {
        //ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (!scripts.TryGetValue(commandTrim, out KahunaScript? script))
        {
            script = connection.LoadScript(commandTrim);
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

    private static async Task LoadAndRunScript(KahunaClient connection, Dictionary<string, KahunaScript> scripts, List<string> history, string commandTrim)
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
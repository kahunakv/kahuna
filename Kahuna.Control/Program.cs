
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using CommandLine;
using DotNext.Threading.Tasks;
using RadLine;
using Spectre.Console;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kommander.Diagnostics;

ParserResult<Options> optsResult = Parser.Default.ParseArguments<Options>(args);

Options? opts = optsResult.Value;
if (opts is null)
    return;

AnsiConsole.MarkupLine("[green]Kahuna Shell 0.0.1 (alpha)[/]\n");

string historyPath = string.Concat(Path.GetTempPath(), Path.PathSeparator, "kahuna.history.json");
List<string> history = await GetHistory(historyPath);

KahunaClient connection = await GetConnection(opts);

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
        // control structures or operators
        "not",
        "let",
        "if",
        "else",
        "then",
        "end",
        "begin",
        "rollback",
        "commit",
        "return",
        "sleep",
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
        "to_string",
        "to_str",
        "is_int",
        "is_long",
        "is_double",
        "is_float",
        "is_boolean",
        "is_string",
        "is_str",
        "revision",
        "length",
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
        AnsiConsole.MarkupLine("[yellow]Disposing lock {0}...[/]", kvp.Value.Owner);

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
            await LoadAndRunScript(commandTrim);
            continue;
        }
        
        if (commandTrim.StartsWith("lock ", StringComparison.InvariantCultureIgnoreCase))
        {
            history.Add(commandTrim);
            
            string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);

            KahunaLock kahunaLock = await connection.GetOrCreateLock(parts[1], int.Parse(parts[2]));

            if (kahunaLock.IsAcquired)
            {
                AnsiConsole.MarkupLine("[cyan]acquired {0} rev:{1}[/]\n", Markup.Escape(Encoding.UTF8.GetString(kahunaLock.Owner)), Markup.Escape(kahunaLock.FencingToken.ToString()));
                
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
                    AnsiConsole.MarkupLine("[cyan]unlocked[/]");
                //else
                //    AnsiConsole.MarkupLine("[yellow]not unlocked[/]");
                
                locks.Remove(parts[1]);
            } 
            else
            {
                AnsiConsole.MarkupLine("[yellow]not acquired[/]");
            }
            
            continue;
        }
        
        if (commandTrim.StartsWith("get-lock ", StringComparison.InvariantCultureIgnoreCase))
        {
            history.Add(commandTrim);
            
            string[] parts = commandTrim.Split(" ", StringSplitOptions.RemoveEmptyEntries);

            if (locks.TryGetValue(parts[1], out KahunaLock? kahunaLock))
            {
                bool success = await connection.Unlock(parts[1], kahunaLock.Owner);

                if (success)
                    AnsiConsole.MarkupLine("[cyan]got {0} rev:{1}[/]", Markup.Escape(Encoding.UTF8.GetString(kahunaLock.Owner)), kahunaLock.FencingToken);
                else
                    AnsiConsole.MarkupLine("[yellow]not acquired[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[yellow]not acquired[/]");
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
                    AnsiConsole.MarkupLine("[cyan]got {0} rev:{1}[/]", Markup.Escape(Encoding.UTF8.GetString(kahunaLock.Owner)), fencingToken);
                else
                    AnsiConsole.MarkupLine("[yellow]not acquired[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[yellow]not acquired[/]");
            }
            
            continue;
        }

        await RunCommand(commandTrim);
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
    }
}


static Task<KahunaClient> GetConnection(Options opts)
{
    string? connectionString = opts.ConnectionSource;

    string[] connectionPool;

    if (string.IsNullOrEmpty(connectionString))
        connectionPool = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];
    else
        connectionPool = connectionString.Split(",", StringSplitOptions.RemoveEmptyEntries).ToArray();

    return Task.FromResult(new KahunaClient(connectionPool, null, new Kahuna.Client.Communication.RestCommunication(null)));
}

static async Task<List<string>> GetHistory(string historyPath)
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

static async Task SaveHistory(string historyPath, List<string>? history)
{
    if (history is not null)
        await File.WriteAllTextAsync(historyPath, JsonSerializer.Serialize(history));
    
    //AnsiConsole.MarkupLine("[cyan]Saving history to {0}...[/]", Markup.Escape(historyPath));
}

async Task RunCommand(string commandTrim)
{
    ValueStopwatch stopwatch = ValueStopwatch.StartNew();

    if (!scripts.TryGetValue(commandTrim, out KahunaScript? script))
    {
        script = connection.LoadScript(commandTrim);
        scripts.Add(commandTrim, script);
    }
    
    KahunaKeyValueTransactionResult result = await script.Run();
            
    switch (result.Type)
    {
        case KeyValueResponseType.Get:
            AnsiConsole.MarkupLine("r{0} [cyan]{1}[/] {2}ms\n", result.Revision, Markup.Escape(Encoding.UTF8.GetString(result.Value ?? [])), stopwatch.GetElapsedMilliseconds());
            break;
        
        case KeyValueResponseType.DoesNotExist:
            AnsiConsole.MarkupLine("r{0} [yellow]not found[/] {1}ms\n", result.Revision, stopwatch.GetElapsedMilliseconds());
            break;
        
        case KeyValueResponseType.Set:
            AnsiConsole.MarkupLine("r{0} [cyan]set[/] {1}ms\n", result.Revision, stopwatch.GetElapsedMilliseconds());
            break;
        
        case KeyValueResponseType.NotSet:
            AnsiConsole.MarkupLine("r{0} [yellow]not set[/] {1}ms\n", result.Revision, stopwatch.GetElapsedMilliseconds());
            break;
        
        case KeyValueResponseType.Deleted:
            AnsiConsole.MarkupLine("r{0} [yellow]deleted[/] {1}ms\n", result.Revision, stopwatch.GetElapsedMilliseconds());
            break;
        
        case KeyValueResponseType.Extended:
            AnsiConsole.MarkupLine("r{0} [yellow]extended[/] {1}ms\n", result.Revision, stopwatch.GetElapsedMilliseconds());
            break;
        
        case KeyValueResponseType.Exists:
            AnsiConsole.MarkupLine("r{0} [yellow]exists[/] {1}ms\n", result.Revision, stopwatch.GetElapsedMilliseconds());
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
            AnsiConsole.MarkupLine("[yellow]{0}[/]ms\n", result.Type, stopwatch.GetElapsedMilliseconds());
            break;
    }
    
    if (result.Type != KeyValueResponseType.Errored && result.Type != KeyValueResponseType.MustRetry)
        history.Add(commandTrim);
}

async Task LoadAndRunScript(string commandTrim)
{
    history.Add(commandTrim);
            
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

        await RunCommand(scriptText);
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
    }
}

public sealed class MyLineNumberPrompt : ILineEditorPrompt
{
    private readonly Style _style;

    public MyLineNumberPrompt(Style? style = null)
    {
        _style = style ?? new Style(foreground: Color.Yellow, background: Color.Blue);
    }

    public (Markup Markup, int Margin) GetPrompt(ILineEditorState state, int line)
    {
        return (new Markup("kahuna-cli> ", _style), 1);
    }
}

public sealed class Options
{
    [Option('c', "connection-source", Required = false, HelpText = "Set the connection string")]
    public string? ConnectionSource { get; set; }
    
    [Option("set", Required = false, HelpText = "Executes a 'set' command")]
    public string? Set { get; set; }
    
    [Option("value", Required = false, HelpText = "Establish the parameter 'value'")]
    public string? Value { get; set; }
}

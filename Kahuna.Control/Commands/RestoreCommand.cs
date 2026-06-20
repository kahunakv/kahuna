using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class RestoreCommand
{
    public static async Task Execute(KahunaClient connection, Guid leafBackupId, string targetDir, long targetTimeMs, string? format)
    {
        KahunaRestoreResponse result = await connection.RestoreAsync(leafBackupId, targetDir, targetTimeMs);

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(result, KahunaJsonContext.Default.KahunaRestoreResponse));
            return;
        }

        AnsiConsole.MarkupLine("Restore completed to [cyan]{0}[/]", Markup.Escape(result.TargetDir));
        AnsiConsole.MarkupLine("  Partitions restored: [green]{0}[/]", result.PartitionsRestored);
        AnsiConsole.MarkupLine("  WAL entries applied: [green]{0}[/]", result.EntriesApplied);
        if (result.LastAppliedPhysicalMs > 0)
            AnsiConsole.MarkupLine("  Last applied time:   [green]{0} ms[/]", result.LastAppliedPhysicalMs);
        AnsiConsole.MarkupLine("\nChain ({0} entries):", result.Chain.Count);
        foreach (KahunaBackupInfo b in result.Chain)
            AnsiConsole.MarkupLine("  [white]{0}[/]  {1}", b.BackupId, b.Type);
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[yellow]Start a fresh node with --storage-path={0} to use this restore.[/]", Markup.Escape(targetDir));
        AnsiConsole.WriteLine();
    }
}

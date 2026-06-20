using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class ListBackupsCommand
{
    public static async Task Execute(KahunaClient connection, string? format)
    {
        List<KahunaBackupInfo> backups = await connection.ListBackupsAsync();

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(backups, KahunaJsonContext.Default.ListKahunaBackupInfo));
            return;
        }

        if (backups.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]No backups found in catalog.[/]");
            return;
        }

        Table table = new();
        table.AddColumn("Backup ID");
        table.AddColumn("Type");
        table.AddColumn("Created (UTC)");
        table.AddColumn("Partitions");
        table.AddColumn("Parent");

        foreach (KahunaBackupInfo b in backups.OrderBy(b => b.CreatedAtUtc))
        {
            string typeMarkup = b.Type == "Full" ? "[green]Full[/]" : "[blue]Incremental[/]";
            table.AddRow(
                b.BackupId.ToString(),
                typeMarkup,
                b.CreatedAtUtc.ToString("u"),
                b.PartitionCount.ToString(),
                b.ParentBackupId?.ToString() ?? "-"
            );
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();
    }
}

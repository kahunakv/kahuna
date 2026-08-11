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
            if (b.IsInvalid)
            {
                // An incomplete backup is called out separately from a corrupt one: the artifacts of a
                // backup that never finished are expected debris an operator can ignore or reclaim,
                // whereas a corrupt manifest on a complete backup is a problem worth investigating.
                table.AddRow(
                    b.BackupId.ToString(),
                    b.IsIncomplete ? "[yellow]INCOMPLETE[/]" : "[red]INVALID[/]",
                    "-",
                    "-",
                    Markup.Escape(b.InvalidReason ?? "unreadable manifest"));
                continue;
            }

            string typeMarkup = b.Type == "Full" ? "[green]Full[/]" : "[blue]Incremental[/]";
            if (b.FormatVersion == 0)
                typeMarkup += " [yellow](legacy)[/]";
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

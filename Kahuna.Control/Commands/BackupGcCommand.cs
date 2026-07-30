using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class BackupGcCommand
{
    public static async Task Execute(KahunaClient connection, bool dryRun, string? format)
    {
        KahunaBackupGcResult result = await connection.RunBackupGarbageCollectionAsync(dryRun);

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(result, KahunaJsonContext.Default.KahunaBackupGcResult));
            return;
        }

        string mode = result.Applied ? "[green]Reclaimed[/]" : "[yellow]Would reclaim (dry run)[/]";
        AnsiConsole.MarkupLine(
            $"{mode}: {result.OrphanReclamations.Count} orphan/leftover artifact(s), " +
            $"{result.RetentionDeletions.Count} retained-out backup(s), {result.BytesReclaimed} byte(s).");

        if (result.RetentionDeletions.Count > 0)
        {
            Table t = new();
            t.AddColumn("Backup ID");
            t.AddColumn("Type");
            t.AddColumn("Created (UTC)");
            t.AddColumn("Bytes");
            t.AddColumn("Reason");
            foreach (KahunaBackupGcDeletion d in result.RetentionDeletions)
                t.AddRow(d.BackupId.ToString(), Markup.Escape(d.Type), d.CreatedAtUtc.ToString("u"),
                    d.Bytes.ToString(), Markup.Escape(d.Reason));
            AnsiConsole.Write(t);
        }

        if (result.OrphanReclamations.Count > 0)
        {
            Table t = new();
            t.AddColumn("Name");
            t.AddColumn("Kind");
            t.AddColumn("Reason");
            foreach (KahunaBackupGcOrphan o in result.OrphanReclamations)
                t.AddRow(Markup.Escape(o.Name), o.IsDirectory ? "dir" : "file", Markup.Escape(o.Reason));
            AnsiConsole.Write(t);
        }

        AnsiConsole.WriteLine();
    }
}

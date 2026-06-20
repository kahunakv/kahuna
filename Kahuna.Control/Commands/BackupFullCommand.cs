using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class BackupFullCommand
{
    public static async Task Execute(KahunaClient connection, string? format)
    {
        KahunaBackupInfo info = await connection.TakeFullBackupAsync();

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(info, KahunaJsonContext.Default.KahunaBackupInfo));
            return;
        }

        AnsiConsole.MarkupLine("Full backup [cyan]{0}[/] completed", info.BackupId);
        PrintInfo(info);
    }

    internal static void PrintInfo(KahunaBackupInfo info)
    {
        AnsiConsole.MarkupLine("  Type:           [green]{0}[/]", info.Type);
        AnsiConsole.MarkupLine("  Created:        [green]{0:u}[/]", info.CreatedAtUtc);
        AnsiConsole.MarkupLine("  Partitions:     [green]{0}[/]", info.PartitionCount);
        if (info.ParentBackupId.HasValue)
            AnsiConsole.MarkupLine("  Parent:         [yellow]{0}[/]", info.ParentBackupId.Value);
        if (info.ClusterSnapshotPhysical.HasValue)
            AnsiConsole.MarkupLine("  Snapshot time:  [blue]{0}ms[/]", info.ClusterSnapshotPhysical.Value);
        AnsiConsole.WriteLine();
    }
}

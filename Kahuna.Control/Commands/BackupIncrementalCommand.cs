using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class BackupIncrementalCommand
{
    public static async Task Execute(KahunaClient connection, Guid parentBackupId, string? format)
    {
        KahunaBackupInfo info = await connection.TakeIncrementalBackupAsync(parentBackupId);

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(info, KahunaJsonContext.Default.KahunaBackupInfo));
            return;
        }

        AnsiConsole.MarkupLine("Incremental backup [cyan]{0}[/] completed", info.BackupId);
        BackupFullCommand.PrintInfo(info);
    }
}

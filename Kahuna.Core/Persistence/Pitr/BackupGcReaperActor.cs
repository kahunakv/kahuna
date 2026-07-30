using Kahuna.Server.Configuration;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Periodically reclaims backup disk: it sweeps orphaned/leftover artifacts (always) and enforces the
/// configured retention policy (delete whole chains beyond the bounds). Without this pass, a node that
/// stops taking backups would never reclaim crash-orphaned artifacts or age out old chains — the inline
/// GC after each backup only fires while backups are still being taken.
/// <para>
/// The first tick fires a short delay after startup, so it doubles as the startup sweep that reclaims
/// artifacts orphaned by a crash before the node went down. Each node runs this against its own local
/// backup directory; the work is serialized against backup creation inside <see cref="BackupService"/>.
/// </para>
/// </summary>
internal sealed class BackupGcReaperActor : IActor<BackupGcReaperRequest>
{
    // Delay before the first (startup) sweep, so it does not contend with boot-time load. The periodic
    // cadence itself comes from configuration.
    private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(15);

    private readonly BackupService backupService;
    private readonly ILogger<IKahuna> logger;

    public BackupGcReaperActor(
        IActorContext<BackupGcReaperActor, BackupGcReaperRequest> context,
        BackupService backupService,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.backupService = backupService;
        this.logger = logger;

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "backup-gc",
            new(),
            StartupDelay,
            configuration.BackupGcInterval);
    }

    public async Task Receive(BackupGcReaperRequest message)
    {
        try
        {
            await backupService.RunGarbageCollectionAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Backup garbage-collection pass failed");
        }
    }
}

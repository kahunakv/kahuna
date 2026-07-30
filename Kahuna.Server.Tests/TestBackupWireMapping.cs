
using System.Text.Json;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies the backup metadata added for typed outcomes survives JSON (the REST wire) round-trips
/// via <see cref="KahunaJsonContext"/> — so a remote REST client observes requested/actual kind,
/// substitution reason, invalid-listing fields, coverage bounds, format version, and restore outcome.
/// </summary>
public sealed class TestBackupWireMapping
{
    [Fact]
    public void BackupInfo_AllMetadata_SurvivesJsonRoundTrip()
    {
        KahunaBackupInfo original = new()
        {
            BackupId = Guid.NewGuid(),
            Type = "Full",
            CreatedAtUtc = DateTime.UtcNow,
            PartitionCount = 3,
            RequestedKind = "Incremental",
            ActualKind = "Full",
            SubstitutionReason = "compaction floor advanced past parent",
            FormatVersion = 1,
            IsInvalid = false,
            InvalidReason = null,
            MinRecoverablePhysicalMs = 100,
            MaxRecoverablePhysicalMs = 500
        };

        string json = JsonSerializer.Serialize(original, KahunaJsonContext.Default.KahunaBackupInfo);
        KahunaBackupInfo? back = JsonSerializer.Deserialize(json, KahunaJsonContext.Default.KahunaBackupInfo);

        Assert.NotNull(back);
        Assert.Equal("Incremental", back!.RequestedKind);
        Assert.Equal("Full", back.ActualKind);
        Assert.Equal(original.SubstitutionReason, back.SubstitutionReason);
        Assert.Equal(1, back.FormatVersion);
        Assert.Equal(100, back.MinRecoverablePhysicalMs);
        Assert.Equal(500, back.MaxRecoverablePhysicalMs);
    }

    [Fact]
    public void BackupInfo_InvalidEntry_SurvivesJsonRoundTrip()
    {
        KahunaBackupInfo original = new()
        {
            BackupId = Guid.NewGuid(),
            IsInvalid = true,
            InvalidReason = "Manifest is not valid JSON",
            FormatVersion = 0
        };

        string json = JsonSerializer.Serialize(original, KahunaJsonContext.Default.KahunaBackupInfo);
        KahunaBackupInfo? back = JsonSerializer.Deserialize(json, KahunaJsonContext.Default.KahunaBackupInfo);

        Assert.NotNull(back);
        Assert.True(back!.IsInvalid);
        Assert.Equal("Manifest is not valid JSON", back.InvalidReason);
    }

    [Fact]
    public void RestoreResponse_OutcomeAndBounds_SurviveJsonRoundTrip()
    {
        KahunaRestoreResponse original = new()
        {
            TargetDir = "/data/restore",
            PartitionsRestored = 2,
            EntriesApplied = 42,
            LastAppliedPhysicalMs = 900,
            Outcome = KahunaBackupOutcome.Ok,
            MinRecoverablePhysicalMs = 100,
            MaxRecoverablePhysicalMs = 900
        };

        string json = JsonSerializer.Serialize(original, KahunaJsonContext.Default.KahunaRestoreResponse);
        KahunaRestoreResponse? back = JsonSerializer.Deserialize(json, KahunaJsonContext.Default.KahunaRestoreResponse);

        Assert.NotNull(back);
        Assert.Equal(KahunaBackupOutcome.Ok, back!.Outcome);
        Assert.Equal(100, back.MinRecoverablePhysicalMs);
        Assert.Equal(900, back.MaxRecoverablePhysicalMs);
    }
}

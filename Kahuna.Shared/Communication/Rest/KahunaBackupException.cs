namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Public exception carrying a stable <see cref="KahunaBackupOutcome"/> so callers can classify a
/// failed backup/restore operation by the enum rather than by inspecting internal exception types
/// or messages. The message is sanitized (no absolute server paths or raw backend exception text).
/// </summary>
public sealed class KahunaBackupException : Exception
{
    public KahunaBackupOutcome Outcome { get; }

    public KahunaBackupException(KahunaBackupOutcome outcome, string message)
        : base(message)
    {
        Outcome = outcome;
    }
}

/// <summary>
/// Wire-contract constants for carrying a <see cref="KahunaBackupOutcome"/> across the REST and gRPC
/// boundaries so a remote client can reconstruct a typed <see cref="KahunaBackupException"/> instead
/// of receiving a generic HTTP 500 / gRPC Unknown.
/// </summary>
public static class KahunaBackupWire
{
    /// <summary>gRPC trailer key carrying the outcome enum name.</summary>
    public const string OutcomeGrpcTrailer = "kahuna-backup-outcome";

    /// <summary>HTTP response header carrying the outcome enum name.</summary>
    public const string OutcomeHttpHeader = "X-Kahuna-Backup-Outcome";
}

using CommandLine;

namespace Kahuna.Control;

/// <summary>
/// Represents the available options for configuring and executing commands
/// via the command-line interface for the Kahuna system. This class
/// provides a variety of optional parameters to customize the behavior
/// of the kahuna-cli tool.
/// </summary>
public sealed class KahunaControlOptions
{
    [Option('c', "connection-source", Required = false, HelpText = "Set the connection string")]
    public string? ConnectionSource { get; set; }

    [Option('t', "default-timeout", Required = false, HelpText = "Sets the default client-side timeout in seconds")]
    public int DefaultTimeout { get; set; } = 10;

    [Option("format", Required = false, HelpText = "Defines the output format (console, json)")]
    public string? Format { get; set; }

    [Option("set", Required = false, HelpText = "Executes a 'set' command")]
    public string? Set { get; set; }

    [Option("get", Required = false, HelpText = "Executes a 'get' command")]
    public string? Get { get; set; }

    [Option("extend", Required = false, HelpText = "Executes a 'extend' command")]
    public string? Extend { get; set; }

    [Option("value", Required = false, HelpText = "Establish the parameter 'value'")]
    public string? Value { get; set; }

    [Option("get-by-prefix", Required = false, HelpText = "Executes a 'get-by-prefix' command")]
    public string? GetByBucket { get; set; }

    [Option("scan-by-prefix", Required = false, HelpText = "Executes a 'scan-by-prefix' command")]
    public string? ScanByPrefix { get; set; }

    [Option("expires", Required = false, HelpText = "Defines the 'expires' parameter")]
    public int Expires { get; set; }

    [Option("lock", Required = false, HelpText = "Acquires a lock by the given name")]
    public string? Lock { get; set; }

    [Option("unlock", Required = false, HelpText = "Unlocks a lock by the given name")]
    public string? Unlock { get; set; }

    [Option("extend-lock", Required = false, HelpText = "Extends the lock by the given name")]
    public string? ExtendLock { get; set; }

    [Option("owner", Required = false, HelpText = "References the current owner of a lock")]
    public string? Owner { get; set; }

    [Option("create-sequence", Required = false, HelpText = "Creates a persistent sequence by the given name")]
    public string? CreateSequence { get; set; }

    [Option("get-sequence", Required = false, HelpText = "Gets sequence information by the given name")]
    public string? GetSequence { get; set; }

    [Option("next-sequence", Required = false, HelpText = "Reserves the next value for the sequence by the given name")]
    public string? NextSequence { get; set; }

    [Option("reserve-sequence", Required = false, HelpText = "Reserves a range of values for the sequence by the given name")]
    public string? ReserveSequence { get; set; }

    [Option("delete-sequence", Required = false, HelpText = "Deletes a persistent sequence by the given name")]
    public string? DeleteSequence { get; set; }

    [Option("initial-value", Required = false, HelpText = "Defines the initial value for a sequence")]
    public long InitialValue { get; set; }

    [Option("increment", Required = false, HelpText = "Defines the increment for a sequence")]
    public long Increment { get; set; } = 1;

    [Option("max-value", Required = false, HelpText = "Defines the optional maximum value for a sequence")]
    public long? MaxValue { get; set; }

    [Option("count", Required = false, HelpText = "Defines the number of sequence values to reserve")]
    public int Count { get; set; } = 1;

    [Option("idempotency-key", Required = false, HelpText = "Defines an optional idempotency key for sequence allocation")]
    public string? IdempotencyKey { get; set; }

    [Option("insecure", Required = false, HelpText = "Skip TLS certificate validation (useful for self-signed certs in dev/standalone mode)")]
    public bool Insecure { get; set; }

    [Option("cluster-members", Required = false, HelpText = "Prints the live cluster membership roster")]
    public bool ClusterMembers { get; set; }

    [Option("backup-full", Required = false, HelpText = "Takes a full backup on the connected node")]
    public bool BackupFull { get; set; }

    [Option("backup-incremental", Required = false, HelpText = "Takes an incremental backup; requires --parent-backup-id")]
    public bool BackupIncremental { get; set; }

    [Option("backup-coordinated", Required = false, HelpText = "Takes a coordinated full backup using the cluster-wide safe snapshot timestamp")]
    public bool BackupCoordinated { get; set; }

    [Option("list-backups", Required = false, HelpText = "Lists all backups in the node's catalog")]
    public bool ListBackups { get; set; }

    [Option("backup-chain", Required = false, HelpText = "Resolves and validates the backup chain for the given leaf backup ID")]
    public Guid? BackupChain { get; set; }

    [Option("parent-backup-id", Required = false, HelpText = "Parent backup ID for incremental backups")]
    public Guid? ParentBackupId { get; set; }

    [Option("restore", Required = false, HelpText = "Restores the backup chain ending at the given leaf backup ID to --target-dir")]
    public Guid? Restore { get; set; }

    [Option("target-dir", Required = false, HelpText = "Target directory for restore output")]
    public string? TargetDir { get; set; }

    [Option("target-time-ms", Required = false, HelpText = "Point-in-time to restore to (ms since Unix epoch; 0 = chain max)")]
    public long TargetTimeMs { get; set; }
}

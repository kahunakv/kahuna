namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// One artifact entry belonging to a backup: its path relative to that backup's root, using
/// forward slashes on every platform, and its byte length.
/// </summary>
public readonly record struct BackupArtifactEntry(string RelativePath, long Length);

/// <summary>
/// What a given artifact store can and cannot do, so callers adapt instead of assuming POSIX
/// filesystem semantics.
/// </summary>
/// <param name="SupportsPosixHardening">
/// True when artifacts live on a local filesystem whose permissions Kahuna controls, so owner-only
/// (0600/0700) restriction and symlink/reparse-point rejection are meaningful and mandatory. False for
/// object storage, where the equivalent protections (bucket policy, blocked public access, server-side
/// encryption) are deployment configuration — a store that returns false must document what it enforces
/// out of band rather than silently dropping a security control.
/// </param>
/// <param name="SupportsCheapRangeReads">
/// True when reading a byte range costs about what reading that many bytes costs (a local seek, or an
/// HTTP range request). False when a partial read is not meaningfully cheaper than a full one, so a
/// caller that would otherwise make several ranged passes should stream once instead.
/// </param>
/// <param name="RequiresLocalScratch">
/// True when the store cannot be written to, or read from, directly by a persistence backend — every
/// checkpoint must transit a local scratch directory. Object stores set this; the local store does not.
/// </param>
public sealed record BackupArtifactStoreCapabilities(
    bool SupportsPosixHardening,
    bool SupportsCheapRangeReads,
    bool RequiresLocalScratch);

/// <summary>
/// A local directory a persistence backend can write a checkpoint into, plus the means to publish it
/// into the artifact store.
/// <para>
/// This exists because <c>IPersistenceBackend.CreateCheckpointAsOf</c> writes a directory tree through
/// the filesystem and can target nothing else — a bucket can never be its direct destination. Rather
/// than force every target to transit scratch (which would make a local backup copy its own checkpoint
/// for no reason), the store decides: the local store hands back the final artifact directory and
/// commits as a no-op, while a remote store hands back scratch and uploads on commit.
/// </para>
/// <para>
/// Dispose without committing to discard the staged bytes. Disposal after a successful commit is a
/// no-op for the local store and drops the scratch tree for a remote one.
/// </para>
/// </summary>
public interface IBackupCheckpointStagingArea : IAsyncDisposable
{
    /// <summary>
    /// Local directory the backend writes the checkpoint tree into. Its <b>parent</b> exists; the path
    /// itself must not, because the backends publish a checkpoint by renaming a sibling temp tree onto it
    /// and that rename fails if anything is already there.
    /// </summary>
    string LocalPath { get; }

    /// <summary>
    /// Publishes everything under <see cref="LocalPath"/> into the store under the backup's
    /// <c>checkpoint/</c> prefix. Must run before the manifest is published, preserving the
    /// artifacts-then-manifest ordering that makes manifest presence the existence predicate.
    /// </summary>
    Task CommitAsync(CancellationToken ct = default);
}

/// <summary>
/// Storage for backup artifact <b>bytes</b> — checkpoint trees and per-partition WAL segments — as
/// opposed to <see cref="IBackupStorageTarget"/>, which stores the manifests describing them. The two
/// are separate contracts because manifest operations are small-object CRUD while artifact operations
/// stream potentially multi-gigabyte objects, with different failure and cost models.
/// <para>
/// Artifacts are addressed by <c>(backupId, relativePath)</c>. There is deliberately <b>no rename or
/// move primitive</b>: no object store can implement one, so the publish protocol must not need it.
/// Publication is instead ordered — artifacts first, manifest last — so a crash mid-backup leaves at
/// worst an orphan artifact set with no manifest, which the sweep reclaims, never a manifest pointing
/// at absent bytes.
/// </para>
/// <para>
/// Every operation is asynchronous and cancellable, and deletes are idempotent: an object store cannot
/// delete a prefix atomically, so reclamation is a listing plus batched deletes that may fail part-way
/// and must converge when re-run.
/// </para>
/// </summary>
public interface IBackupArtifactStore
{
    /// <summary>What this store can do; see <see cref="BackupArtifactStoreCapabilities"/>.</summary>
    BackupArtifactStoreCapabilities Capabilities { get; }

    /// <summary>True when the backup has any artifact bytes at all.</summary>
    Task<bool> BackupExistsAsync(Guid backupId, CancellationToken ct = default);

    /// <summary>True when this exact artifact exists.</summary>
    Task<bool> ExistsAsync(Guid backupId, string relativePath, CancellationToken ct = default);

    /// <summary>
    /// Every artifact belonging to <paramref name="backupId"/>, with sizes, ordinal-sorted by path.
    /// Returns an empty list when the backup has no artifacts. Implementations must page internally
    /// rather than assuming one response covers the whole prefix.
    /// <para>
    /// A store with POSIX hardening also enforces its safety rules here — a symlink/reparse point
    /// anywhere in the tree is an error, never a silently-followed link.
    /// </para>
    /// </summary>
    Task<IReadOnlyList<BackupArtifactEntry>> ListAsync(Guid backupId, CancellationToken ct = default);

    /// <summary>
    /// Opens an artifact for reading. <paramref name="offset"/> and <paramref name="length"/> select a
    /// byte range; the default reads the whole object. Callers that would make several ranged passes
    /// should check <see cref="BackupArtifactStoreCapabilities.SupportsCheapRangeReads"/> first and
    /// stream once instead when ranges are expensive.
    /// </summary>
    Task<Stream> OpenReadAsync(
        Guid backupId, string relativePath, long offset = 0, long? length = null, CancellationToken ct = default);

    /// <summary>
    /// Opens an artifact for writing. Publication is explicit: bytes written to
    /// <see cref="IBackupArtifactWriter.Stream"/> become a visible artifact only when
    /// <see cref="IBackupArtifactWriter.CompleteAsync"/> is called, and disposing without completing
    /// discards them. That way a writer abandoned by an exception mid-stream cannot publish a truncated
    /// artifact. Implementations choose their own mechanism (temp file plus atomic rename locally,
    /// single-shot or multipart upload remotely); either way a reader never observes a partial object.
    /// </summary>
    Task<IBackupArtifactWriter> OpenWriteAsync(Guid backupId, string relativePath, CancellationToken ct = default);

    /// <summary>
    /// Ids of every backup that currently has artifact bytes, whether or not a manifest exists for it.
    /// This is the orphan sweep's candidate set; ownership is decided against
    /// <see cref="IBackupStorageTarget.ListManifestIdsAsync"/>, never against this listing alone.
    /// <para>
    /// Must fail rather than return a partial or empty result when the underlying listing is
    /// unavailable: an unreadable listing read as "no artifacts" would let the sweep reclaim live data.
    /// </para>
    /// </summary>
    Task<IReadOnlyList<Guid>> ListBackupIdsAsync(CancellationToken ct = default);

    /// <summary>
    /// Opaque handles for leftover debris that belongs to no backup id — the remnants of an interrupted
    /// publish, delete, or restore. What counts as debris is store-specific and deliberately not
    /// modelled here: the local store recognises its own temp/staging/quarantine naming, while a store
    /// whose writes are atomic may legitimately have no debris class at all and return nothing. Pass a
    /// returned handle to <see cref="DeleteLeftoverAsync"/>; treat it as meaningful to no one else.
    /// </summary>
    Task<IReadOnlyList<BackupArtifactLeftover>> ListLeftoversAsync(CancellationToken ct = default);

    /// <summary>Removes one leftover reported by <see cref="ListLeftoversAsync"/>. Idempotent.</summary>
    Task DeleteLeftoverAsync(BackupArtifactLeftover leftover, CancellationToken ct = default);

    /// <summary>
    /// Removes every artifact belonging to <paramref name="backupId"/>. Idempotent and safe to re-run
    /// against a partially deleted backup: a store that cannot delete atomically will leave some
    /// objects behind on failure, and the next pass must finish the job rather than throw or skip.
    /// Callers tombstone the manifest first, so a crash here never strands a resolvable manifest.
    /// </summary>
    Task DeleteAllAsync(Guid backupId, CancellationToken ct = default);

    /// <summary>
    /// Begins a checkpoint for <paramref name="backupId"/>, returning the local directory the
    /// persistence backend should write into. See <see cref="IBackupCheckpointStagingArea"/> for why
    /// this indirection exists.
    /// </summary>
    Task<IBackupCheckpointStagingArea> BeginCheckpointAsync(Guid backupId, CancellationToken ct = default);

    /// <summary>
    /// Materialises every artifact under <paramref name="relativePrefix"/> into
    /// <paramref name="destinationDirectory"/> on the local filesystem, preserving relative paths
    /// exactly — a persistence backend opens the result and is layout-sensitive (RocksDB expects
    /// <c>{dir}/{revision}/</c>, SQLite expects files directly in <c>{dir}/</c>), so flattening or
    /// reordering keys breaks the open.
    /// <para>
    /// <paramref name="throttleBytesPerSec"/> paces the transfer when positive so a large restore does
    /// not saturate the disk or link and starve foreground traffic; 0 means unthrottled. The caller
    /// verifies the materialised bytes against the manifest before opening them, so a partial or
    /// tampered transfer is caught before it reaches a backend.
    /// </para>
    /// </summary>
    Task MaterializeAsync(
        Guid backupId,
        string relativePrefix,
        string destinationDirectory,
        long throttleBytesPerSec = 0,
        CancellationToken ct = default);
}

/// <summary>
/// An in-progress artifact write. Bytes go to <see cref="Stream"/>; the artifact becomes visible only
/// after <see cref="CompleteAsync"/>. Disposing without completing discards the write, so an exception
/// mid-stream cannot leave a truncated artifact behind a valid-looking path.
/// </summary>
public interface IBackupArtifactWriter : IAsyncDisposable
{
    /// <summary>Write-only stream receiving the artifact bytes.</summary>
    Stream Stream { get; }

    /// <summary>
    /// Publishes everything written so far as the complete artifact. After this returns the artifact is
    /// visible to readers and listings. Calling it more than once is an error.
    /// </summary>
    Task CompleteAsync(CancellationToken ct = default);
}

/// <summary>
/// A store-specific handle to leftover artifact debris. <see cref="Handle"/> is opaque and only
/// meaningful to the store that produced it; <see cref="Name"/> and <see cref="Description"/> are for
/// operator output and must not leak absolute server paths or credentials.
/// </summary>
/// <param name="IsDirectory">
/// True when this leftover is a container of other entries rather than a single object. Stores with no
/// directory notion report false.
/// </param>
public readonly record struct BackupArtifactLeftover(
    string Handle, string Name, string Description, bool IsDirectory);

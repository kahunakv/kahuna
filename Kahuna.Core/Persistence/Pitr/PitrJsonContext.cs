
using System.Text.Json.Serialization;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Source-generated serialization metadata for the compact JSON files the checkpoint and backup
/// paths write: WAL segment lines, the checkpoint sidecar and the memory-backend checkpoint image.
/// Generated metadata keeps these paths working in a trimmed build, where reflection-based
/// serialization loses the members it needs. The options match the reflection defaults these files
/// were always written with, so existing files and segment digests stay byte-compatible.
/// <para>
/// Metadata-only generation: the generated fast-path writer writes a null byte array as an empty
/// string, where reflection wrote <c>null</c>. A null value and an empty value are different states,
/// so these files use the metadata writer, which matches reflection.
/// </para>
/// </summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(WalSegmentEntry))]
[JsonSerializable(typeof(CheckpointManifest))]
[JsonSerializable(typeof(List<MemoryPersistenceBackend.MemoryCheckpointEntry>))]
[JsonSerializable(typeof(List<MemoryPersistenceBackend.MemoryCheckpointLockEntry>))]
internal sealed partial class PitrJsonContext : JsonSerializerContext;

/// <summary>
/// Source-generated serialization metadata for backup manifests. Manifests are written indented
/// because operators read them directly on disk.
/// </summary>
[JsonSourceGenerationOptions(WriteIndented = true, GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(BackupManifest))]
internal sealed partial class PitrIndentedJsonContext : JsonSerializerContext;

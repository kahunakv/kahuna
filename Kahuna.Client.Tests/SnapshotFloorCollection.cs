
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Tests;

/// <summary>
/// Serializes all snapshot-hold tests relative to every other test collection.
/// The snapshot floor is cluster-global state: concurrent test variants that acquire
/// and release holds against the same floor value corrupt each other's assertions.
/// DisableParallelization ensures each test method (and all its theory cases) completes
/// fully before the next one starts, making "floor drops to zero when last hold released"
/// assertions reliable.
/// </summary>
[CollectionDefinition("SnapshotFloorTests", DisableParallelization = true)]
public sealed class SnapshotFloorCollection;

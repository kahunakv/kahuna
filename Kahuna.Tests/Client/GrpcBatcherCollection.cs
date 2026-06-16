
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Tests.Client;

/// <summary>
/// Serializes tests that read process-global GrpcBatcher state (requestRefs / PendingRequestCount)
/// relative to all other test collections, so that concurrent gRPC traffic from other test
/// classes does not perturb the delta assertions.
/// </summary>
[CollectionDefinition("GrpcBatcherTests", DisableParallelization = true)]
public sealed class GrpcBatcherCollection;

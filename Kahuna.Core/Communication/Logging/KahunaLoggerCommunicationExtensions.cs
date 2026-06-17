/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Communication.External.Grpc.Logging;

public static partial class KahunaLoggerCommunicationExtensions
{
    [LoggerMessage(Level = LogLevel.Trace, Message = "IOException")]
    public static partial void LogCommunicationIoException(this ILogger<IKahuna> logger, Exception exception);
}

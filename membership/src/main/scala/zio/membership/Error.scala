package zio.membership

import zio.duration.Duration
import zio.nio.SocketAddress

// TODO: define error hierarchy
sealed abstract class Error(msg: String = "", cause: Throwable = null) extends Exception(msg, cause)

sealed abstract class TransportError(msg: String = "", cause: Throwable = null) extends Error(msg, cause)

final case class ExceptionThrown(exc: Throwable) extends TransportError(cause = exc)

final case class RequestTimeout(timeout: Duration) extends TransportError(msg = s"Request timeout $timeout.")

final case class BindFailed(addr: SocketAddress, exc: Throwable)
    extends TransportError(msg = s"Failed binding to address $addr.", cause = exc)

final case class SerializationError(msg: String, cause: Throwable = null) extends Error(msg, cause)
final case class DeserializationError(msg: String, cause: Throwable = null) extends Error(msg, cause)

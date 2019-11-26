package zio.membership
import zio.duration.Duration
import zio.nio.SocketAddress

// TODO: define error hierarchy
sealed abstract class Error(msg: String = "", cause: Throwable = null) extends Exception(msg, cause)

sealed abstract class SendError(msg: String = "", cause: Throwable = null) extends Error(msg, cause)

object SendError {
  final case class ExceptionThrown(exc: Throwable) extends SendError(cause = exc)
  final case class Timeout(timeout: Duration)      extends SendError(msg = s"Violated timeout $timeout.")
}

sealed abstract class ReceiveError(msg: String = "", cause: Throwable = null) extends Error(msg, cause)

object ReceiveError {

  final case class BindFailed(addr: SocketAddress, exc: Throwable)
      extends ReceiveError(msg = s"Failed binding to address $addr.", cause = exc)
  final case class ExceptionThrown(exc: Throwable) extends ReceiveError(cause = exc)
}

final case class DeserializationError(msg: String) extends ReceiveError

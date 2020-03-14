package zio.membership
import zio.duration.Duration
import zio.membership.transport.Address

sealed abstract class Error(msg: String = "", cause: Throwable = null) extends Exception(msg, cause)

sealed abstract class TransportError(msg: String = "", cause: Throwable = null)  extends Error(msg, cause)
sealed abstract class SendError(msg: String = "", cause: Throwable = null)       extends Error(msg, cause)
final case class SerializationError(msg: String = "", cause: Throwable = null)   extends Error(msg, cause)
final case class DeserializationError(msg: String = "", cause: Throwable = null) extends Error(msg, cause)

final case class ResolutionFailed(address: Address, cause: Throwable)
    extends Error(s"Resolution failed for $address", cause)

object TransportError {
  final case class MaxConnectionsReached(n: Int) extends TransportError(msg = s"Reached max connections: $n")

  final case class ExceptionThrown(exc: Throwable) extends TransportError(msg = exc.getMessage, cause = exc)

  final case class ConnectionTimeout(timeout: Duration) extends TransportError(msg = s"Connection timed out $timeout.")
  final case class RequestTimeout(timeout: Duration)    extends TransportError(msg = s"Request timeout $timeout.")

  final case class BindFailed(addr: Address, exc: Throwable)
      extends TransportError(msg = s"Failed binding to address $addr.", cause = exc)
}

object SendError {
  case object NotConnected extends SendError

  final case class SerializationFailed(err: zio.keeper.SerializationError) extends SendError(msg = err.msg)
  final case class TransportFailed(err: zio.membership.TransportError)     extends SendError(cause = err)
}

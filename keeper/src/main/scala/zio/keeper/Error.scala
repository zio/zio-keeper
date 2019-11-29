package zio.keeper

import zio.duration.Duration
import zio.nio.SocketAddress

//sealed trait Error extends Exception
sealed abstract class Error(msg: String = "", cause: Throwable = null) extends Exception(msg, cause)

object Error {

  case class ConnectionTimeout(addr: SocketAddress, timeout: Duration) extends Error
  case class RequestTimeout(addr: SocketAddress)                       extends Error

  case class CannotFindSerializerForMessage[A](obj: A)    extends Error
  case class CannotFindSerializerForMessageId(msgId: Int) extends Error

  case class SerializationError(msg: String)   extends Error
  case class DeserializationError(msg: String) extends Error

  case class NodeUnknown(nodeId: NodeId)                             extends Error
  case class SendError[A](nodeId: NodeId, message: A, error: String) extends Error
  case class HandshakeError(msg: String)                             extends Error

  case class ServiceDiscoveryError(msg: String) extends Error
  // TODO: define error hierarchy
}

sealed abstract class TransportError(msg: String = "", cause: Throwable = null) extends Error(msg = msg, cause = cause)

final case class ExceptionThrown(exc: Throwable)   extends TransportError(cause = exc)
final case class RequestTimeout(timeout: Duration) extends TransportError(msg = s"Request timeout $timeout.")

final case class BindFailed(addr: SocketAddress, exc: Throwable)
    extends TransportError(msg = s"Failed binding to address $addr.", cause = exc)

//final case class DeserializationError(msg: String) extends Error

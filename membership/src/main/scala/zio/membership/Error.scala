package zio.membership
import zio.nio.SocketAddress

sealed trait Error extends Exception
object Error {
  case class BindFailed(addr: SocketAddress, msg: String) extends Error
  case class ConnectionTimeout(addr: SocketAddress) extends Error
  case class RequestTimeout(addr: SocketAddress)    extends Error

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

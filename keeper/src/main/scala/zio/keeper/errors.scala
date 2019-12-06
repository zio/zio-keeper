package zio.keeper

import zio.duration.Duration
import zio.keeper.membership.NodeId
import zio.keeper.protocol.InternalProtocol
import zio.nio.SocketAddress

import scala.reflect.ClassTag

sealed abstract class Error(val msg: String = "") {
  override def toString: String = msg
}

sealed abstract class SerializationError(msg: String = "") extends Error(msg = msg)

object SerializationError {

  final case class SerializationTypeError[A](val cause: Throwable)(implicit ct: ClassTag[A])
      extends SerializationError(
        msg = s"Cannot serialize ${ct.runtimeClass.getCanonicalName} because of ${cause.getMessage}"
      )

  final case class DeserializationTypeError[A](val cause: Throwable)(implicit ct: ClassTag[A])
      extends SerializationError(
        msg = s"Cannot deserialize ${ct.runtimeClass.getCanonicalName} because of ${cause.getMessage}"
      )
}

final case class ServiceDiscoveryError(override val msg: String) extends Error

sealed abstract class ClusterError(msg: String = "") extends Error(msg = msg)

object ClusterError {

  final case class SendError[A](nodeId: NodeId, message: A, error: TransportError)
      extends ClusterError(msg = s"Failed to send message[$message] to $nodeId")

  final case class HandshakeError(addr: SocketAddress, error: Error)
      extends ClusterError(msg = s"Connection handshake for $addr failed with ${error.msg}")

  final case class UnexpectedMessage(message: Message) extends ClusterError

  final case class AckMessageFail(ackId: Long, message: InternalProtocol, to: NodeId)
      extends ClusterError(msg = s"message [$message] with ack id: $ackId sent to: $to overdue timeout ")

  final case class UnknownNode(nodeId: NodeId) extends ClusterError(msg = nodeId.toString + " is not in cluster")
}

sealed abstract class TransportError(msg: String = "") extends Error(msg = msg)

object TransportError {

  final case class ExceptionWrapper(throwable: Throwable)
      extends TransportError(msg = if (throwable.getMessage == null) throwable.toString else throwable.getMessage)

  final case class RequestTimeout(addr: SocketAddress, timeout: Duration)
      extends TransportError(msg = s"Request timeout $timeout for connection [$addr].")

  final case class ConnectionTimeout(addr: SocketAddress, timeout: Duration)
      extends TransportError(msg = s"Connection timeout $timeout to [$addr].")

  final case class BindFailed(addr: SocketAddress, exc: Throwable)
      extends TransportError(msg = s"Failed binding to address $addr.")

  final case class ChannelClosed(socketAddress: SocketAddress)
      extends TransportError(msg = s"Channel to $socketAddress is closed")
}

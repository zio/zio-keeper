package zio.keeper

import zio.duration.Duration
import zio.nio.SocketAddress

import scala.reflect.ClassTag

sealed abstract class Error(val msg: String = "")

sealed abstract class AbstractSerializationError(msg: String = "") extends Error(msg = msg)

case class SerializationTypeError[A](val cause: scala.Throwable)(implicit ct: ClassTag[A])
    extends AbstractSerializationError(
      msg = s"Cannot serialize ${ct.runtimeClass.getCanonicalName} because of ${cause.getMessage}"
    )

case class DeserializationTypeError[A](val cause: scala.Throwable)(implicit ct: ClassTag[A])
    extends AbstractSerializationError(
      msg = s"Cannot deserialize ${ct.runtimeClass.getCanonicalName} because of ${cause.getMessage}"
    )

case class ServiceDiscoveryError(override val msg: String) extends Error

sealed abstract class ClusterError(msg: String = "") extends Error(msg = msg)

case class SendError[A](nodeId: NodeId, message: A, error: TransportError)
    extends ClusterError(msg = s"Failed to send message[$message] to $nodeId")

case class HandshakeError(addr: SocketAddress, error: Error)
    extends ClusterError(msg = s"Connection handshake for $addr failed with ${error.msg}")
case class UnexpectedMessage(message: Message) extends ClusterError

sealed abstract class TransportError(msg: String = "") extends Error(msg = msg)

final case class TransportExceptionWrapper(throwable: Throwable) extends TransportError(msg = throwable.getMessage)

final case class RequestTimeout(addr: SocketAddress, timeout: Duration)
    extends TransportError(msg = s"Request timeout $timeout for connection [$addr].")

final case class ConnectionTimeout(addr: SocketAddress, timeout: Duration)
    extends TransportError(msg = s"Connection timeout $timeout to [$addr].")

final case class BindFailed(addr: SocketAddress, exc: Throwable)
    extends TransportError(msg = s"Failed binding to address $addr.")

final case class ChannelClosed(socketAddress: SocketAddress)
    extends TransportError(msg = s"Channel to $socketAddress is closed")

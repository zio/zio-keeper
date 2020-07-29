package zio.keeper

import zio.duration.Duration
import zio.keeper.transport.Address
import zio.nio.core.SocketAddress

sealed abstract class Error(val msg: String = "", val cause: Throwable = null) extends Exception(msg, cause)

sealed abstract class SerializationError(msg: String = "") extends Error(msg = msg)

object SerializationError {

  final case class SerializationTypeError(msg0: String)
      extends SerializationError(
        msg = msg0
      )

  object SerializationTypeError {

    def apply(cause: Throwable): SerializationTypeError =
      SerializationTypeError(s"Cannot serialize because of ${cause.getMessage}")
  }

  final case class DeserializationTypeError(msg0: String)
      extends SerializationError(
        msg = msg0
      )

  object DeserializationTypeError {

    def apply(cause: Throwable): DeserializationTypeError =
      new DeserializationTypeError(s"Cannot deserialize because of ${cause.getMessage}")
  }

}

final case class ServiceDiscoveryError(override val msg: String) extends Error

sealed abstract class ClusterError(msg: String = "") extends Error(msg = msg)

object ClusterError {

  final case class UnknownNode(nodeId: NodeAddress) extends ClusterError(msg = nodeId.toString + " is not in cluster")

}

sealed abstract class SendError(msg: String = "") extends Error(msg = msg)

object SendError {
  case object NotConnected extends SendError

  final case class SerializationFailed(err: SerializationError) extends SendError(msg = err.msg)
  final case class TransportFailed(err: TransportError)         extends SendError(msg = err.msg)
}

sealed abstract class TransportError(msg: String = "", cause: Throwable = null) extends Error(msg, cause)

object TransportError {

  final case class ExceptionWrapper(throwable: Throwable)
      extends TransportError(msg = "Exception in transport", cause = throwable)

  final case class MaxConnectionsReached(n: Int) extends TransportError(msg = s"Reached max connections: $n")

  final case class RequestTimeout(timeout: Duration) extends TransportError(msg = s"Request timeout after $timeout.")

  final case class ConnectionTimeout(timeout: Duration)
      extends TransportError(msg = s"Connection timeout after $timeout.")

  final case class BindFailed(addr: SocketAddress, exc: Throwable)
      extends TransportError(msg = s"Failed binding to address $addr.")

  final case class ChannelClosed(socketAddress: SocketAddress)
      extends TransportError(msg = s"Channel to $socketAddress is closed")

  final case class ResolutionFailed(address: Address) extends TransportError(s"Resolution failed for $address")

}

object SwimError {

  case class SuspicionTimeoutCancelled(nodeAddress: NodeAddress)
      extends Error(s"Suspicion timeout for node: $nodeAddress has been cancelled")
}

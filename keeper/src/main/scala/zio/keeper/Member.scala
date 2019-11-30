package zio.keeper

import java.util.UUID

import zio.ZIO
import zio.nio.{ InetAddress, InetSocketAddress, SocketAddress }

final case class NodeId(value: UUID) extends AnyVal

object NodeId {
  implicit val ordering: Ordering[NodeId] = Ordering.by(_.value)

  def generateNew: NodeId =
    NodeId(UUID.randomUUID())
}

final case class Member(nodeId: NodeId, addr: NodeAddress)

object Member {
  implicit val ordering: Ordering[Member] = Ordering.by(_.nodeId)
}

final case class NodeAddress(ip: Array[Byte], port: Int) {

  def socketAddress: ZIO[Any, TransportError, InetSocketAddress] =
    (for {
      addr <- InetAddress.byAddress(ip)
      sa   <- SocketAddress.inetSocketAddress(addr, port)
    } yield sa).mapError(ExceptionThrown)
}

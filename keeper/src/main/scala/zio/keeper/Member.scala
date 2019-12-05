package zio.keeper

import java.util.UUID

import zio.ZIO
import zio.nio.{ InetAddress, InetSocketAddress, SocketAddress }
import zio.keeper.TransportError._

final case class NodeId(value: UUID) extends AnyVal

object NodeId {
  implicit val ordering: Ordering[NodeId] = Ordering.by(_.value)

  def generateNew: NodeId =
    NodeId(UUID.randomUUID())
}

final case class Member(nodeId: NodeId, addr: NodeAddress) {
  override def toString: String = s"nodeId: ${nodeId.value}, ip: ${addr.ip.mkString(".")}, port: ${addr.port}"
}

object Member {
  implicit val ordering: Ordering[Member] = Ordering.by(_.nodeId)
}

final case class NodeAddress(ip: Array[Byte], port: Int) {

  override def equals(obj: Any): Boolean = obj match {
    case NodeAddress(ip, port) => this.port == port && ip.sameElements(this.ip)
  }

  def socketAddress: ZIO[Any, TransportError, InetSocketAddress] =
    (for {
      addr <- InetAddress.byAddress(ip)
      sa   <- SocketAddress.inetSocketAddress(addr, port)
    } yield sa).mapError(ExceptionWrapper)
}

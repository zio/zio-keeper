package zio.keeper.membership

import java.util.UUID

import upickle.default._
import zio.ZIO
import zio.keeper.TransportError
import zio.keeper.TransportError._
import zio.nio.{InetAddress, InetSocketAddress, SocketAddress}

final case class NodeId(value: UUID) extends AnyVal

object NodeId {
  implicit val ordering: Ordering[NodeId] = Ordering.by(_.value)

  implicit val nodeIdRW = macroRW[NodeId]

  def generateNew: NodeId =
    NodeId(UUID.randomUUID())
}

final case class Member[A](nodeId: NodeId, addr: A) {
  override def toString: String = s"nodeId: ${nodeId.value}, addr: $addr"
}

object Member {
  implicit def ordering[A]: Ordering[Member[A]] = Ordering.by(_.nodeId)
  implicit def memberRW[A: ReadWriter] = macroRW[Member[A]]
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

package zio.keeper

import java.util.UUID

import zio.nio.InetSocketAddress

final case class NodeId(value: UUID) extends AnyVal

object NodeId {
  implicit val ordering: Ordering[NodeId] = Ordering.by(_.value)

  def generateNew: NodeId =
    NodeId(UUID.randomUUID())
}

final case class Member(nodeId: NodeId, addr: InetSocketAddress)
object Member {
  implicit val ordering: Ordering[Member] = Ordering.by(_.nodeId)

}

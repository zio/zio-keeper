package zio.keeper

import java.util.UUID

import zio.nio.InetSocketAddress

final case class NodeId(value: UUID) extends AnyVal

object NodeId {

  def generateNew: NodeId =
    NodeId(UUID.randomUUID())
}

final case class Member(nodeId: NodeId, addr: InetSocketAddress)

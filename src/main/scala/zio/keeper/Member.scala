package zio.keeper

import java.util.UUID

import zio.nio.InetSocketAddress

final case class NodeId(value: UUID) extends AnyVal
final case class Member(nodeId: NodeId, addr: InetSocketAddress)

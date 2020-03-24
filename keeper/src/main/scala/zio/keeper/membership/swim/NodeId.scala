package zio.keeper.membership.swim

import java.util.UUID

import upickle.default._
import zio.keeper.membership.ByteCodec

case class NodeId(uuid: UUID)

object NodeId {
  def generate = NodeId(UUID.randomUUID())

  implicit val codec: ByteCodec[NodeId] =
    ByteCodec.fromReadWriter(macroRW[NodeId])

  implicit val nodeIdRW: ReadWriter[NodeId] =
    macroRW[NodeId]
}

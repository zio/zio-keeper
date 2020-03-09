package zio.keeper.membership.swim

import java.util.UUID

import zio.Chunk
import upickle.default._
import zio.keeper.ByteCodec

case class Message1(
  nodeId: NodeId,
  payload: Chunk[Byte],
  correlationId: UUID
)

object Message1 {
  def apply: Message.Direct[Chunk[Byte]] => Message1 = input => Message1(input.nodeId, input.message, UUID.randomUUID())

  implicit val codec: ByteCodec[Message1] =
    ByteCodec.fromReadWriter(macroRW[Message1])

  implicit val chunkRW: ReadWriter[Chunk[Byte]] =
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[Chunk[Byte]](
        ch => ch.toArray,
        arr => Chunk.fromArray(arr)
      )
}

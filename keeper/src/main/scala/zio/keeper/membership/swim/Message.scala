package zio.keeper.membership.swim

import java.util.UUID

import zio.Chunk
import upickle.default._
import zio.keeper.ByteCodec

case class Message(
                    nodeId: NodeId,
                    payload: Chunk[Byte],
                    correlationId: UUID
                  )

object Message {
  def apply: ((NodeId, Chunk[Byte])) => Message = input =>
    Message(input._1, input._2, UUID.randomUUID())

  implicit val codec: ByteCodec[Message] =
    ByteCodec.fromReadWriter(macroRW[Message])

  implicit val chunkRW: ReadWriter[Chunk[Byte]] =
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[Chunk[Byte]](
        ch => ch.toArray,
        arr => Chunk.fromArray(arr)
      )
}

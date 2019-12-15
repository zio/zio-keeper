package zio.keeper

import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import zio.{ Chunk, IO }
import zio.keeper.membership.{ Member, NodeId }
import zio.keeper.transport.ChannelOut
import zio.nio.Buffer

final case class Message(
  sender: NodeId,
  payload: Chunk[Byte]
)

object Message {

  private val HeaderSize = 24

  private[keeper] def readMessage(channel: ChannelOut) =
    channel.read.flatMap(
      headerBytes =>
        (for {
          byteBuffer             <- Buffer.byte(headerBytes)
          senderMostSignificant  <- byteBuffer.getLong
          senderLeastSignificant <- byteBuffer.getLong
          messageType            <- byteBuffer.getInt
          payloadByte            <- byteBuffer.getChunk()
          sender                 = NodeId(new java.util.UUID(senderMostSignificant, senderLeastSignificant))
        } yield (messageType, Message(sender, payloadByte)))
          .mapError(e => DeserializationTypeError[Message](e))
    )

  private[keeper] def serializeMessage(member: Member, payload: Chunk[Byte], messageType: Int): IO[Error, Chunk[Byte]] = {
    for {
      byteBuffer <- Buffer.byte(HeaderSize + payload.length)
      _          <- byteBuffer.putLong(member.nodeId.value.getMostSignificantBits)
      _          <- byteBuffer.putLong(member.nodeId.value.getLeastSignificantBits)
      _          <- byteBuffer.putInt(messageType)
      _          <- byteBuffer.putChunk(payload)
      _          <- byteBuffer.flip
      bytes      <- byteBuffer.getChunk()
    } yield bytes
  }.mapError(ex => SerializationTypeError[Message](ex))
}

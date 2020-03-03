package zio.keeper

import java.util.UUID

import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import zio.keeper.membership.{ Member, NodeId }
import zio.keeper.transport.ChannelOut
import zio.nio.core.Buffer
import zio.{ Chunk, IO }

final case class Message(
  id: UUID,
  sender: NodeId,
  payload: Chunk[Byte]
)

object Message {

  private val HeaderSize = 40

  private[keeper] def readMessage(channel: ChannelOut): IO[Error, (Int, Message)] =
    channel.read.flatMap(
      headerBytes =>
        (for {
          byteBuffer             <- Buffer.byte(headerBytes)
          idMostSignificant      <- byteBuffer.getLong
          idLeastSignificant     <- byteBuffer.getLong
          senderMostSignificant  <- byteBuffer.getLong
          senderLeastSignificant <- byteBuffer.getLong
          messageType            <- byteBuffer.getInt
          payloadByte            <- byteBuffer.getChunk()
          id                     = new UUID(idMostSignificant, idLeastSignificant)
          sender                 = NodeId(new UUID(senderMostSignificant, senderLeastSignificant))
        } yield (messageType, Message(id, sender, payloadByte)))
          .mapError(e => DeserializationTypeError[Message](e))
    )

  private[keeper] def serializeMessage(
    messageId: UUID,
    member: Member,
    payload: Chunk[Byte],
    messageType: Int
  ): IO[Error, Chunk[Byte]] = {
    for {
      byteBuffer <- Buffer.byte(HeaderSize + payload.length)
      _          <- byteBuffer.putLong(messageId.getMostSignificantBits)
      _          <- byteBuffer.putLong(messageId.getLeastSignificantBits)
      _          <- byteBuffer.putLong(member.nodeId.value.getMostSignificantBits)
      _          <- byteBuffer.putLong(member.nodeId.value.getLeastSignificantBits)
      _          <- byteBuffer.putInt(messageType)
      _          <- byteBuffer.putChunk(payload)
      _          <- byteBuffer.flip
      bytes      <- byteBuffer.getChunk()
    } yield bytes
  }.mapError(ex => SerializationTypeError[Message](ex))
}

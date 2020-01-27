package zio.keeper

import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import zio.keeper.membership.{ Member, NodeId }
import zio.keeper.transport.ChannelOut
import zio.nio.Buffer
import zio.{ Chunk, IO }

final case class Message(
  id: String,
  sender: NodeId,
  payload: Chunk[Byte]
)

object Message {

  private val HeaderSize = 32

  private[keeper] def readMessage(channel: ChannelOut): IO[Error, (Int, Message)] =
    channel.read.flatMap(
      headerBytes =>
        (for {
          byteBuffer             <- Buffer.byte(headerBytes)
          senderMostSignificant  <- byteBuffer.getLong
          senderLeastSignificant <- byteBuffer.getLong
          messageType            <- byteBuffer.getInt
          idLength               <- byteBuffer.getInt
          idChunk                <- byteBuffer.getChunk(idLength)
          payloadByte            <- byteBuffer.getChunk()
          sender                 = NodeId(new java.util.UUID(senderMostSignificant, senderLeastSignificant))
          id                     = new String(idChunk.toArray)
        } yield (messageType, Message(id, sender, payloadByte)))
          .mapError(e => DeserializationTypeError[Message](e))
    )

  private[keeper] def serializeMessage(
    messageId: String,
    member: Member,
    payload: Chunk[Byte],
    messageType: Int
  ): IO[Error, Chunk[Byte]] = {
    val idChunk = Chunk.fromArray(messageId.getBytes())
    for {
      byteBuffer <- Buffer.byte(HeaderSize + payload.length)
      _          <- byteBuffer.putLong(member.nodeId.value.getMostSignificantBits)
      _          <- byteBuffer.putLong(member.nodeId.value.getLeastSignificantBits)
      _          <- byteBuffer.putInt(messageType)
      _          <- byteBuffer.putInt(idChunk.length)
      _          <- byteBuffer.putChunk(idChunk)
      _          <- byteBuffer.putChunk(payload)
      _          <- byteBuffer.flip
      bytes      <- byteBuffer.getChunk()
    } yield bytes
  }.mapError(ex => SerializationTypeError[Message](ex))
}

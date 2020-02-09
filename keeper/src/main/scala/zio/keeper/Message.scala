package zio.keeper

import zio.keeper.SerializationError.{DeserializationTypeError, SerializationTypeError}
import zio.keeper.membership.NodeId
import zio.keeper.transport.Connection
import zio.nio.Buffer
import zio.{Chunk, IO}

final case class Message(
  sender: NodeId,
  payload: Chunk[Byte]
)

object Message {

  private val HeaderSize = 24

  private[keeper] def readMessage(channel: Connection) =
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
          .mapError(e => DeserializationTypeError(e))
    )

  private[keeper] def serializeMessage(nodeId: NodeId, payload: Chunk[Byte], messageType: Int): IO[Error, Chunk[Byte]] = {
    for {
      byteBuffer <- Buffer.byte(HeaderSize + payload.length)
      _          <- byteBuffer.putLong(nodeId.value.getMostSignificantBits)
      _          <- byteBuffer.putLong(nodeId.value.getLeastSignificantBits)
      _          <- byteBuffer.putInt(messageType)
      _          <- byteBuffer.putChunk(payload)
      _          <- byteBuffer.flip
      bytes      <- byteBuffer.getChunk()
    } yield bytes
  }.mapError(ex => SerializationTypeError(ex))
}

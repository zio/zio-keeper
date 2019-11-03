package zio.keeper

import zio.{Chunk, ZIO}
import zio.keeper.Error.SerializationError
import zio.nio.Buffer
import zio.IO
import zio.keeper.Error.DeserializationError

final case class Message(
  sender: NodeId,
  payload: Chunk[Byte]
)


final case class OpenConnection(state: GossipState, address: Member) {
  def serialize: IO[SerializationError, Chunk[Byte]] = {
    for {
      byteBuffer <- Buffer.byte(24 * (state.members.size + 1) + 4)
      _ <- InternalProtocol.writeMember(address, byteBuffer)
      _ <- byteBuffer.putInt(state.members.size)
      _ <- ZIO.foldLeft(state.members)(byteBuffer) {
            case (acc, member) =>
              InternalProtocol.writeMember(member, acc)
          }
      _ <- byteBuffer.flip
      chunk <- byteBuffer.getChunk()
    } yield chunk
  }.catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))
}

object OpenConnection {
  def deserialize(bytes: Chunk[Byte]) =
    if (bytes.isEmpty) {
      ZIO.fail(SerializationError("Fail to deserialize message"))
    } else {
      (for {
        bb         <- Buffer.byte(bytes)
        addr       <- InternalProtocol.readMember(bb)
        size  <- bb.getInt
        state <- ZIO.foldLeft(1 to size)(GossipState.Empty) { case (acc, _) => InternalProtocol.readMember(bb).map(acc.addMember) }
      } yield OpenConnection(state, addr)).mapError(ex => DeserializationError(ex.getMessage))
    }
}

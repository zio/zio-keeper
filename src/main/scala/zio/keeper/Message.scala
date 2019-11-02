package zio.keeper

import zio.{Chunk, ZIO}
import zio.keeper.Error.SerializationError
import zio.nio.Buffer

final case class Message(sender: NodeId, payload: Chunk[Byte])

final case class OpenConnection(member: Member) {
}
object OpenConnection {

  def deserialize(bytes: Chunk[Byte]) =
    if (bytes.isEmpty) {
      ZIO.fail(SerializationError("Fail to deserialize message"))
    } else {
      Buffer.byte(bytes).flatMap(InternalProtocol.readMember).map(OpenConnection(_))
    }

}

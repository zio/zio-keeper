package zio.keeper.protocol

import java.math.BigInteger

import upickle.default._
import zio.keeper.Error.SerializationError
import zio.keeper.{ GossipState, Member, NodeAddress, NodeId }
import zio.{ Chunk, IO, ZIO }

sealed trait InternalProtocol {

  final val serialize: IO[SerializationError, Chunk[Byte]] =
    ZIO
      .effect(Chunk.fromArray(writeBinary(this)))
      .mapError(ex => SerializationError(ex.toString))
}

object InternalProtocol {

  def deserialize(bytes: Chunk[Byte]): IO[SerializationError, InternalProtocol] =
    if (bytes.isEmpty) {
      ZIO.fail(SerializationError("Fail to deserialize message"))
    } else {
      ZIO
        .effect(
          readBinary[InternalProtocol](bytes.toArray)
        )
        .mapError(ex => SerializationError(ex.getMessage))
    }

  final case class Ack(conversation: Long, state: GossipState) extends InternalProtocol

  final case class Ping(ackConversation: Long, state: GossipState) extends InternalProtocol

  final case class PingReq(target: Member, ackConversation: Long, state: GossipState) extends InternalProtocol

  final case class OpenConnection(state: GossipState, address: Member) extends InternalProtocol

  implicit val gossipStateRW: ReadWriter[GossipState] = macroRW[GossipState]

  implicit val memberRW: ReadWriter[Member] = macroRW[Member]

  implicit val nodeIdRW: ReadWriter[NodeId] = macroRW[NodeId]

  implicit val inetAddressRW: ReadWriter[NodeAddress] =
    readwriter[Array[Byte]].bimap[NodeAddress](
      addr => addr.ip ++ BigInt.apply(addr.port).toByteArray,
      arr => NodeAddress(arr.take(4), new BigInteger(arr.drop(4)).intValue()) //This still can fail...
    )

  implicit val internalProtocolRW: ReadWriter[InternalProtocol] = ReadWriter.merge(
    macroRW[Ack],
    macroRW[Ping],
    macroRW[PingReq],
    macroRW[OpenConnection]
  )
}

package zio.keeper

import java.util.UUID

import zio.{ Chunk, IO, ZIO }
import zio.keeper.Error.{ DeserializationError, SerializationError }
import zio.nio.{ Buffer, ByteBuffer, InetAddress, SocketAddress }

sealed trait InternalProtocol {
  def serialize: IO[SerializationError, Chunk[Byte]]
}

object InternalProtocol {

  private val AckMsgId: Byte     = 1
  private val PingMsgId: Byte    = 2
  private val PingReqMsgId: Byte = 3

  def readMember(byteBuffer: ByteBuffer) =
    for {
      ms          <- byteBuffer.getLong
      ls          <- byteBuffer.getLong
      ip          <- byteBuffer.getChunk(4)
      port        <- byteBuffer.getInt
      inetAddress <- InetAddress.byAddress(ip.toArray)
      addr        <- SocketAddress.inetSocketAddress(inetAddress, port)
    } yield Member(NodeId(new UUID(ms, ls)), addr)

  def writeMember(member: Member, byteBuffer: ByteBuffer) =
    for {
      _        <- byteBuffer.putLong(member.nodeId.value.getMostSignificantBits)
      _        <- byteBuffer.putLong(member.nodeId.value.getLeastSignificantBits)
      inetAddr <- InetAddress.byName(member.addr.hostString)
      _        <- byteBuffer.putChunk(Chunk.fromArray(inetAddr.address))
      _        <- byteBuffer.putInt(member.addr.port)
    } yield byteBuffer

  def deserialize(bytes: Chunk[Byte]) =
    if (bytes.isEmpty) {
      ZIO.fail(SerializationError("Fail to deserialize message"))
    } else {
      val messageByte = bytes.drop(1)
      bytes.apply(0) match {
        case PingMsgId =>
          (for {
            bb    <- Buffer.byte(messageByte)
            id    <- bb.getLong
            size  <- bb.getInt
            state <- ZIO.foldLeft(1 to size)(GossipState.Empty) { case (acc, _) => readMember(bb).map(acc.addMember) }
          } yield Ping(id, state)).mapError(ex => DeserializationError(ex.getMessage))
        case PingReqMsgId =>
          (for {
            bb     <- Buffer.byte(messageByte.drop(4))
            id     <- bb.getLong
            target <- readMember(bb)
            size   <- bb.getInt
            state  <- ZIO.foldLeft(1 to size)(GossipState.Empty) { case (acc, _) => readMember(bb).map(acc.addMember) }
          } yield PingReq(target, id, state)).mapError(ex => DeserializationError(ex.getMessage))
        case AckMsgId =>
          (for {
            bb    <- Buffer.byte(messageByte)
            id    <- bb.getLong
            size  <- bb.getInt
            state <- ZIO.foldLeft(1 to size)(GossipState.Empty) { case (acc, _) => readMember(bb).map(acc.addMember) }
          } yield Ack(id, state)).mapError(ex => DeserializationError(ex.getMessage))
      }
    }

  final case class Ack(conversation: Long, state: GossipState) extends InternalProtocol {

    override val serialize: IO[SerializationError, Chunk[Byte]] =
      (for {
        byteBuffer <- Buffer.byte(1 + 8 + 4 + 24 * state.members.size)
        _          <- byteBuffer.put(AckMsgId)
        _          <- byteBuffer.putLong(conversation)
        _          <- byteBuffer.putInt(state.members.size)
        _ <- ZIO.foldLeft(state.members)(byteBuffer) {
              case (acc, member) =>
                writeMember(member, acc)
            }
        _     <- byteBuffer.flip
        chunk <- byteBuffer.getChunk()
      } yield chunk).catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))
  }

  final case class Ping(ackConversation: Long, state: GossipState) extends InternalProtocol {

    override val serialize: IO[SerializationError, Chunk[Byte]] =
      (for {
        byteBuffer <- Buffer.byte(1 + 8 + 4 + 24 * state.members.size)
        _          <- byteBuffer.put(PingMsgId)
        _          <- byteBuffer.putLong(ackConversation)
        _          <- byteBuffer.putInt(state.members.size)
        _ <- ZIO.foldLeft(state.members)(byteBuffer) {
              case (acc, member) =>
                writeMember(member, acc)
            }
        _     <- byteBuffer.flip
        chunk <- byteBuffer.getChunk()
      } yield chunk).catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))
  }

  final case class PingReq(target: Member, ackConversation: Long, state: GossipState) extends InternalProtocol {

    override def serialize: IO[SerializationError, Chunk[Byte]] =
      (for {
        byteBuffer <- Buffer.byte(1 + 8 + 4 + 24 * (state.members.size + 1))
        _          <- byteBuffer.put(PingReqMsgId)
        _          <- byteBuffer.putLong(ackConversation)
        _          <- writeMember(target, byteBuffer)
        _          <- byteBuffer.putInt(state.members.size)
        _ <- ZIO.foldLeft(state.members)(byteBuffer) {
              case (acc, member) =>
                writeMember(member, acc)
            }
        _     <- byteBuffer.flip
        chunk <- byteBuffer.getChunk()
      } yield chunk).catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))
  }

  final case class OpenConnection(state: GossipState, address: Member) {

    def serialize: IO[SerializationError, Chunk[Byte]] = {
      for {
        byteBuffer <- Buffer.byte(24 * (state.members.size + 1) + 4)
        _          <- InternalProtocol.writeMember(address, byteBuffer)
        _          <- byteBuffer.putInt(state.members.size)
        _ <- ZIO.foldLeft(state.members)(byteBuffer) {
              case (acc, member) =>
                InternalProtocol.writeMember(member, acc)
            }
        _     <- byteBuffer.flip
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
          bb   <- Buffer.byte(bytes)
          addr <- InternalProtocol.readMember(bb)
          size <- bb.getInt
          state <- ZIO.foldLeft(1 to size)(GossipState.Empty) {
                    case (acc, _) => InternalProtocol.readMember(bb).map(acc.addMember)
                  }
        } yield OpenConnection(state, addr)).mapError(ex => DeserializationError(ex.getMessage))
      }
  }
}

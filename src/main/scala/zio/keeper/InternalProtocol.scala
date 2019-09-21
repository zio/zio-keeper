package zio.keeper

import java.math.BigInteger
import java.util.UUID

import zio.{Chunk, IO, ZIO}
import zio.keeper.Error.SerializationError
import zio.nio.{Buffer, ByteBuffer, InetAddress, InetSocketAddress, SocketAddress}

sealed trait InternalProtocol {
  def serialize: IO[SerializationError, Chunk[Byte]]
}

object InternalProtocol {

  private val RequestClusterStateMsgId: Byte = 1
  private val ProvideClusterStateMsgId: Byte = 2
  private val NotifyJoinMsgId: Byte          = 3
  private val AckMsgId: Byte                 = 4

  private def readMember(byteBuffer: ByteBuffer) =
    for {
      ms          <- byteBuffer.getLong
      ls          <- byteBuffer.getLong
      ip          <- byteBuffer.getChunk(4)
      port        <- byteBuffer.getInt
      inetAddress <- InetAddress.byAddress(ip.toArray)
      addr        <- SocketAddress.inetSocketAddress(inetAddress, port)
    } yield Member(NodeId(new UUID(ms, ls)), addr)

  private def writeMember(member: Member, byteBuffer: ByteBuffer) =
    for {
      _        <- byteBuffer.putLong(member.nodeId.value.getMostSignificantBits)
      _        <- byteBuffer.putLong(member.nodeId.value.getLeastSignificantBits)
      inetAddr <- InetAddress.byName(member.addr.hostString)
      _        <- byteBuffer.putChunk(Chunk.fromArray(inetAddr.address))
      _        <- byteBuffer.putInt(member.addr.port)
    } yield byteBuffer

  def deserialize(bytes: Chunk[Byte]): ZIO[Any, Throwable, InternalProtocol] =
    if (bytes.isEmpty) {
      ZIO.fail(SerializationError("Fail to deserialize message"))
    } else {
      val messageByte = bytes.drop(1)
      bytes.apply(0) match {
        case RequestClusterStateMsgId =>
          ZIO.succeed(RequestClusterState)
        case ProvideClusterStateMsgId =>
          for {
            bb    <- Buffer.byte(messageByte)
            size  <- bb.getInt
            state <- ZIO.foldLeft(1 to size)(GossipState.Empty) { case (acc, _) => readMember(bb).map(acc.addMember) }
          } yield ProvideClusterState(state)
        case NotifyJoinMsgId =>
          for {
            a   <- InetAddress.byAddress(messageByte.take(4).toArray)
            res <- ZIO.effect(new BigInteger(messageByte.drop(4).toArray).intValue())
            socketAddr <- SocketAddress.inetSocketAddress(a, res)
          } yield NotifyJoin(socketAddr)
        case AckMsgId =>
          ZIO.succeed(Ack)
      }
    }

  final case class ProvideClusterState(state: GossipState) extends InternalProtocol {
    override def serialize: IO[SerializationError, Chunk[Byte]] = {
      for {
        byteBuffer <- Buffer.byte(1 + 24 * state.members.size + 4)
        _          <- byteBuffer.put(InternalProtocol.ProvideClusterStateMsgId)
        _          <- byteBuffer.putInt(state.members.size)
        _ <- ZIO.foldLeft(state.members)(byteBuffer) {
          case (acc, member) =>
            writeMember(member, acc)
        }
        _     <- byteBuffer.flip
        chunk <- byteBuffer.getChunk()
      } yield chunk
      }.catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))
  }

  final case class NotifyJoin(addr: InetSocketAddress) extends InternalProtocol {
    override def serialize: IO[SerializationError, Chunk[Byte]] =
      (for {
        inetAddr <- InetAddress.byName(addr.hostString)
      } yield {
        Chunk(NotifyJoinMsgId.toByte) ++
          Chunk.fromArray(inetAddr.address) ++
          Chunk.fromArray(BigInteger.valueOf(addr.port.toLong).toByteArray)
      }).catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))


  }

  case object RequestClusterState extends InternalProtocol {
    override val serialize: IO[SerializationError, Chunk[Byte]] =
      ZIO.succeed(Chunk(RequestClusterStateMsgId))
  }

  case object Ack extends InternalProtocol {
    override val serialize: IO[SerializationError, Chunk[Byte]] =
      ZIO.succeed(Chunk(AckMsgId))
  }

}

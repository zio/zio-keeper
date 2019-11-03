package zio.keeper

import zio._
import zio.console.Console
import zio.keeper.Error._
import zio.nio._
import zio.nio.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel }
import zio.stream.Stream

trait Cluster {
  def nodes: UIO[List[NodeId]]

  def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit]

  def broadcast(data: Chunk[Byte]): IO[Error, Unit]

  def receive: Stream[Error, Message]
}

object Cluster {

  private val HeaderSize = 24

  def join[A](
    port: Int
  ): ZManaged[
    Credentials with Discovery with Transport with zio.console.Console with zio.clock.Clock with zio.random.Random,
    Error,
    Cluster
  ] =
    InternalCluster.initCluster(port)

  private[keeper] def readMessage(channel: AsynchronousSocketChannel) =
    (for {
      headerBytes            <- channel.read(HeaderSize)
      byteBuffer             <- Buffer.byte(headerBytes)
      senderMostSignificant  <- byteBuffer.getLong
      senderLeastSignificant <- byteBuffer.getLong
      messageType            <- byteBuffer.getInt
      payloadSize            <- byteBuffer.getInt
      payloadByte            <- channel.read(payloadSize)
      sender                 = NodeId(new java.util.UUID(senderMostSignificant, senderLeastSignificant))
    } yield (messageType, Message(sender, payloadByte))).mapError(ex => DeserializationError(ex.getMessage()))

  private[keeper] def serializeMessage(member: Member, payload: Chunk[Byte], messageType: Int): IO[Error, Chunk[Byte]] = {
    for {
      byteBuffer <- Buffer.byte(HeaderSize + payload.length)
      _          <- byteBuffer.putLong(member.nodeId.value.getMostSignificantBits)
      _          <- byteBuffer.putLong(member.nodeId.value.getLeastSignificantBits)
      _          <- byteBuffer.putInt(messageType)
      _          <- byteBuffer.putInt(payload.length)
      _          <- byteBuffer.putChunk(payload)
      _          <- byteBuffer.flip
      bytes      <- byteBuffer.getChunk()
    } yield bytes
  }.catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))

  trait Credentials {
    // TODO: ways to obtain auth data
  }

  trait Discovery {
    def discover: ZIO[Console, Error, Set[zio.nio.SocketAddress]]
  }

  trait Transport {
    def bind(publicAddress: InetSocketAddress): Managed[Throwable, AsynchronousServerSocketChannel]

    def connect(ip: SocketAddress): Managed[Throwable, AsynchronousSocketChannel]
  }

  object Transport {

    trait TCPTransport extends Transport {

      override def bind(publicAddress: InetSocketAddress): Managed[Throwable, AsynchronousServerSocketChannel] =
        for {
          socket <- AsynchronousServerSocketChannel()
          _      <- socket.bind(publicAddress).toManaged_
        } yield socket

      override def connect(ip: SocketAddress): Managed[Throwable, AsynchronousSocketChannel] =
        for {
          client <- AsynchronousSocketChannel()
          _      <- client.connect(ip).toManaged_
        } yield client
    }
  }
}

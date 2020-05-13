package zio.keeper.membership

import zio._
import zio.keeper.{ ByteCodec, NodeAddress, TransportError }
import zio.keeper.membership.PingPong.{ Ping, Pong }
import zio.keeper.membership.swim.Messages.WithPiggyback
import zio.keeper.membership.swim.{ Broadcast, Message, Messages, Protocol }
import zio.keeper.transport.{ Bind, Channel, ConnectionLessTransport }
import zio.logging.Logging
import zio.nio.core.SocketAddress
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

object MessagesSpec extends DefaultRunnableSpec {

  val logger = Logging.console((_, line) => line)

  class TestTransport(in: Queue[Channel], out: Queue[(SocketAddress, Chunk[Byte])])
      extends ConnectionLessTransport.Service {

    override def bind(
      localAddr: SocketAddress
    )(connectionHandler: Channel => UIO[Unit]): Managed[TransportError, Bind] =
      ZStream
        .fromQueue(in)
        .foreach(conn => connectionHandler(conn) *> conn.close)
        .fork
        .as(new Bind(in.isShutdown, in.shutdown, ZIO.succeed(localAddr)))
        .toManaged(_.close.ignore)

    override def connect(to: SocketAddress): Managed[TransportError, Channel] =
      ZManaged.succeed(
        new Channel(
          _ => ZIO.succeed(Chunk.empty),
          chunk => out.offer((to, chunk)).unit,
          ZIO.succeed(true),
          ZIO.unit
        )
      )

    def simulateNewConnection[A: ByteCodec](message: Message.Direct[A]) =
      for {
        queue <- ZQueue.unbounded[Byte]
        _ <- ByteCodec
              .encode(message.message)
              .map(WithPiggyback(message.node, _, List.empty))
              .flatMap(ByteCodec[WithPiggyback].toChunk)
              .map { chunk =>
                val size = chunk.size
                Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte) ++ chunk
              }
              .flatMap(chunk => chunk.mapM(queue.offer))

        read = (size: Int) => queue.takeUpTo(size).map(Chunk.fromIterable)

        connection = new Channel(read, _ => ZIO.unit, ZIO.succeed(true), queue.shutdown)

        _ <- in.offer(connection)
      } yield ()

    def sentMessages = ZStream.fromQueue(out)
  }

  object TestTransport {

    def make =
      for {
        in  <- Queue.bounded[Channel](100).toManaged(_.shutdown)
        out <- Queue.bounded[(SocketAddress, Chunk[Byte])](100).toManaged(_.shutdown)
      } yield new TestTransport(in, out)

  }

  val messages = for {
    local     <- NodeAddress.local(1111).toManaged_
    transport <- TestTransport.make
    broadcast <- Broadcast.make(64000, 2).toManaged_
    messages  <- Messages.make(local, broadcast, transport)
  } yield (transport, messages)

  val protocol = Protocol[PingPong].make(
    {
      case Message.Direct(sender, Ping(i)) =>
        ZIO.succeed(Message.Direct(sender, Pong(i)))
      case _ => Message.noResponse
    },
    ZStream.empty
  )

  val spec = suite("messages")(
    testM("receiveMessage") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)

      messages.use {
        case (testTransport, messages) =>
          for {
            dl <- protocol
            _  <- messages.process(dl.binary)
            _  <- testTransport.simulateNewConnection(Message.Direct(testNodeAddress, PingPong.Ping(123): PingPong))
            _  <- testTransport.simulateNewConnection(Message.Direct(testNodeAddress, PingPong.Ping(321): PingPong))
            m <- testTransport.sentMessages
                  .mapM {
                    case (_, chunk) =>
                      ByteCodec[WithPiggyback].fromChunk(chunk.drop(4))
                  }
                  .mapM {
                    case WithPiggyback(_, chunk, _) => ByteCodec.decode[PingPong](chunk)
                  }
                  .take(2)
                  .runCollect
          } yield assert(m)(hasSameElements(List(PingPong.Pong(123), PingPong.Pong(321))))
      }
    }
  ).provideCustomLayer(logger)

}

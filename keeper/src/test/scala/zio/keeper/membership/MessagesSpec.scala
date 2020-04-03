package zio.keeper.membership

import upickle.default.macroRW
import zio.keeper.membership.swim.protocols.Initial.Join
import zio.keeper.membership.swim.{Message, Messages, Protocol}
import zio.keeper.transport.Channel.Connection
import zio.keeper.transport.{Channel, Transport}
import zio.keeper.{TransportError, transport}
import zio.logging.Logging
import zio.nio.core.{InetSocketAddress, SocketAddress}
import zio.stream.{Take, ZStream}
import zio.test.Assertion._
import zio.test._
import zio._
import zio.logging._
import zio.duration._
import zio.keeper.membership.PingPong.{Ping, Pong}

object MessagesSpec extends DefaultRunnableSpec {

  val logger = Logging.console((_, line) => line)



  class TestTransport(
                       in: Queue[Connection],
                       out: Queue[(SocketAddress, Chunk[Byte])]) extends Transport.Service {

    override def bind(
      localAddr: SocketAddress
    )(connectionHandler: Channel.Connection => UIO[Unit]): Managed[TransportError, Channel.Bind] =
      ZStream.fromQueue(in)
        .foreach(conn => connectionHandler(conn) *> conn.close)
        .fork
        .as(new Channel.Bind(in.isShutdown, in.shutdown, ZIO.succeed(localAddr))).toManaged(_.close.ignore)

    override def connect(to: SocketAddress): Managed[TransportError, Channel.Connection] =
      ZManaged.succeed(new Connection(
        _ => ZIO.succeed(Chunk.empty),
        chunk => out.offer((to, chunk)).unit,
        ZIO.succeed(true),
        ZIO.unit
      ))


    def simulateNewConnection[A: TaggedCodec](message: Message.Direct[A]) =
      for {
        queue <- ZQueue.unbounded[Byte]
        _ <- TaggedCodec.write(message.message).map(Message.Direct(message.node, _))
            .flatMap(ByteCodec[Message.Direct[Chunk[Byte]]].toChunk)
              .map { chunk =>
                val size = chunk.size
                Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte) ++ chunk
              }.flatMap(chunk => chunk.mapM(queue.offer))

        read = (size: Int) =>
          queue.takeUpTo(size).map(Chunk.fromIterable)

        connection = new Connection(read, _ => ZIO.unit, ZIO.succeed(true), queue.shutdown)

        _ <- in.offer(connection)
      } yield ()

    def sentMessages = ZStream.fromQueue(out)
  }

  object TestTransport {
    def make = for {
      in       <- Queue.bounded[Connection](100).toManaged(_.shutdown)
      out       <- Queue.bounded[(SocketAddress, Chunk[Byte])](100).toManaged(_.shutdown)
    } yield new TestTransport(in, out)

  }

  val messages = for {
    local     <- NodeAddress.local(1111).toManaged_
    transport <- TestTransport.make
    messages <- Messages.make(local, transport)
  } yield (transport, messages)

  val protocol = Protocol[PingPong](
    {
      case Message.Direct(sender, Ping(i)) =>
        ZIO.succeed(Option(Message.Direct(sender, Pong(i))))
    },
    ZStream.empty
  )

  val spec = suite("messages")(
    testM("receiveMessage") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)

      messages.tap(_._2.bind).use { case (testTransport, messages) =>
        for {
          dl <- protocol
          f <- messages.process(dl.binary).fork
          _ <- testTransport.simulateNewConnection(Message.Direct(testNodeAddress, PingPong.Ping(123): PingPong))
          m <- testTransport.sentMessages.mapM { case (_, chunk) =>
            ByteCodec[Message.Direct[Chunk[Byte]]].fromChunk(chunk.drop(4))
          }.mapM{
            case Message.Direct(_, chunk) => TaggedCodec.read[PingPong](chunk)
          }.runHead
        _ <- f.join
        } yield assert(m)(isSome(equalTo(PingPong.Pong(123))))

      }
    }
  ).provideCustomLayer(logger)

}

package zio.keeper.membership.swim

import zio._
import zio.keeper.Error
import zio.keeper.membership.{ByteCodec, NodeAddress}
import zio.keeper.transport.Channel.Connection
import zio.keeper.transport.Transport
import zio.logging.Logging.Logging
import zio.logging.log
import zio.stream.{Take, ZStream}

class Messages(
  val local: NodeAddress,
  messages: Queue[Take[Error, Message[Chunk[Byte]]]],
  broadcast: Broadcast,
  transport: Transport.Service
) {

  /**
   * Reads message and put into queue.
   *
   * @param connection transport connection
   */
  final def read(connection: Connection): IO[Error, Unit] =
    Take
      .fromEffect(
        connection.read >>= ByteCodec[Message.WithPiggyback].fromChunk
      )
      .flatMap {
        case Take.Value(withPiggyback) =>
          messages.offer(Take.Value(Message.Direct(withPiggyback.node, withPiggyback.message))) *>
            ZIO.foreach(withPiggyback.gossip)(
              chunk => messages.offer(Take.Value(Message.Direct(withPiggyback.node, chunk)))
            )
        case other => messages.offer(other)
      }
      .unit

  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging, Nothing, Take[E, A]] =
    stream.either.mapM(
      _.fold(
        e => log.error("error during sending", Cause.fail(e)).as(Take.Fail(Cause.fail(e))),
        v => ZIO.succeedNow(Take.Value(v))
      )
    )

  /**
   * Sends message to target.
   */
  final def send(msg: Message.Direct[Chunk[Byte]]): IO[Error, Unit] =
    for {
      broadcast     <- broadcast.broadcast(msg.message.size)
      withPiggyback = Message.WithPiggyback(local, msg.message, broadcast)
      chunk         <- ByteCodec[Message.WithPiggyback].toChunk(withPiggyback)
      nodeAddress   <- msg.node.socketAddress
      _             <- transport.connect(nodeAddress).use(_.send(chunk))
    } yield ()

  def bind =
    for {
      localAddress <- local.socketAddress.toManaged_
      _            <- log.info("bind to " + localAddress).toManaged_
      logger       <- ZManaged.environment[Logging]
      _ <- transport
            .bind(localAddress) { conn =>
              read(conn)
                .catchAll(ex => log.error("fail to read", Cause.fail(ex)).unit.provide(logger))
            }
    } yield ()

  final def process(protocol: Protocol[Chunk[Byte]]) =
    ZStream.mergeAll(2)(
      ZStream
        .fromQueue(messages)
        .collectM {
          case Take.Value(msg: Message.Direct[Chunk[Byte]]) =>
            Take.fromEffect(protocol.onMessage(msg))
        },
      recoverErrors(protocol.produceMessages)
    ).collectM {
        case Take.Value(msg: Message.Direct[Chunk[Byte]]) =>
          send(msg)
            .catchAll(e => log.error("error during send: " + e))
        case Take.Value(msg: Message.Batch[Chunk[Byte]]) =>
          ZIO.foreach(msg.first :: msg.second :: Nil) {
            case msg @ Message.Broadcast(_) =>
              broadcast.add(msg)
            case msg @ Message.Direct(_, _) =>
              send(msg)
                .catchAll(e => log.error("error during send: " + e))
          }
        case Take.Fail(cause) =>
          log.error("error: ", cause)
      }
      .runDrain
}

object Messages {

  def make(
    local: NodeAddress,
    broadcast: Broadcast,
    udpTransport: Transport.Service
  ) =
    for {
      messageQueue <- Queue
                       .bounded[Take[Error, Message[Chunk[Byte]]]](1000)
                       .toManaged(_.shutdown)
      messages = new Messages(local, messageQueue, broadcast, udpTransport)
      _        <- messages.bind
    } yield messages

}

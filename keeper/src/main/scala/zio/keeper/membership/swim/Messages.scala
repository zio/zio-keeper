package zio.keeper.membership.swim

import zio._
import zio.keeper.Error
import zio.keeper.membership.{ ByteCodec, NodeAddress }
import zio.keeper.transport.Channel.Connection
import zio.keeper.transport.Transport
import zio.logging.Logging.Logging
import zio.logging.log
import zio.stream.{ Take, ZStream }

class Messages(
  val local: NodeAddress,
  messages: Queue[Take[Error, Message.Direct[Chunk[Byte]]]],
  transport: Transport.Service
) {

  /**
   * Reads message and put into queue.
   *
   * @param connection transport connection
   */
  final def read(connection: Connection): ZIO[Any, Error, Unit] =
    Take
      .fromEffect(
        connection.read
          .flatMap(ByteCodec[Message.Direct[Chunk[Byte]]].fromChunk)
      )
      .flatMap(messages.offer)
      .unit

  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging, Nothing, Take[E, Option[A]]] =
    stream.either.mapM(
      _.fold(
        e => log.error("error during sending", Cause.fail(e)).as(Take.Fail(Cause.fail(e))),
        v => ZIO.succeedNow(Take.Value(Some(v)))
      )
    )

  /**
   * Sends message to target.
   */
  final def send(msg: Message.Direct[Chunk[Byte]]): IO[Error, Unit] =
    ByteCodec[Message.Direct[Chunk[Byte]]]
      .toChunk(msg.copy(node = local))
      .flatMap(
        chunk => msg.node.socketAddress.toManaged_.flatMap(transport.connect).use(_.send(chunk))
      )

  def bind =
    for {
      localAddress <- local.socketAddress.toManaged_
      _            <- log.info("bind to " + localAddress).toManaged_
      logger       <- ZManaged.environment[Logging]
      _ <- transport
            .bind(localAddress) { conn =>
              read(conn)
                .tap(_ => log.info("reading").provide(logger))
                .catchAll(ex => log.error("fail to read", Cause.fail(ex)).unit.provide(logger))
            }
    } yield ()

  final def process(protocol: Protocol[Chunk[Byte]]) =
    ZStream
      .fromQueue(messages)
      .collectM {
        case Take.Value(msg) =>
          Take.fromEffect(
            protocol.onMessage(msg)
          )
      }
      .collectM {
        case Take.Value(Some(msg: Message.Direct[Chunk[Byte]])) =>
          log.debug("sending") *>
            send(msg)
              .catchAll(e => log.error("error during send: " + e))
        case Take.Fail(cause) =>
          log.error("error: ", cause)
        case Take.End =>
          log.error("end")
        case res => log.error("res: " + res)
      }
      .runDrain
      .fork *> recoverErrors(protocol.produceMessages)
      .collectM {
        case Take.Value(Some(msg: Message.Direct[Chunk[Byte]])) =>
          log.debug("sending") *>
            send(msg)
              .catchAll(e => log.error("error during send: " + e))
        case Take.Fail(cause) =>
          log.error("error: ", cause)
        case Take.End =>
          log.error("end")
        case res => log.error("res: " + res)
      }
      .runDrain
      .fork
}

object Messages {

  def make(
    local: NodeAddress,
    udpTransport: Transport.Service
  ) =
    for {
      messageQueue <- Queue
                       .bounded[Take[Error, Message.Direct[Chunk[Byte]]]](1000)
                       .toManaged(_.shutdown)
      messages = new Messages(local, messageQueue, udpTransport)
      _        <- messages.bind
    } yield messages

}

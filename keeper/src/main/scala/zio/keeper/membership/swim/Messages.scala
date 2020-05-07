package zio.keeper.membership.swim

import upickle.default.macroRW
import zio._
import zio.clock.Clock
import zio.keeper.{ ByteCodec, Error, NodeAddress }
import zio.keeper.membership.swim.Message.WithTimeout
import zio.keeper.transport.Channel
import zio.keeper.transport.ConnectionLessTransport
import zio.logging.Logging
import zio.logging.log
import zio.stream.{ Take, ZStream }
import upickle.default._
import zio.keeper.membership.swim.Messages.WithPiggyback

/**
 * Represents main messages loop. responsible for receiving and sending messages.
 * @param local - local node
 * @param messages - internal queue used to process incomming messages
 * @param broadcast - broadcast for messages
 * @param transport - UDP transport
 */
final class Messages(
  val local: NodeAddress,
  messages: Queue[Take[Error, Message[Chunk[Byte]]]],
  broadcast: Broadcast,
  transport: ConnectionLessTransport.Service
) {

  /**
   * Reads message and put into queue.
   *
   * @param connection transport connection
   */
  def read(connection: Channel): IO[Error, Unit] =
    Take
      .fromEffect(
        connection.read >>= ByteCodec[WithPiggyback].fromChunk
      )
      .flatMap {
        case Take.Value(withPiggyback) =>
          messages.offer(Take.Value(Message.Direct(withPiggyback.node, withPiggyback.message))) *>
            ZIO.foreach(withPiggyback.gossip)(
              chunk => messages.offer(Take.Value(Message.Direct(withPiggyback.node, chunk)))
            )
        case err: Take.Fail[Error] => messages.offer(err)
        case Take.End              => messages.offer(Take.End)
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
  def send(msg: Message[Chunk[Byte]]): ZIO[Clock with Logging, Error, Unit] =
    msg match {
      case Message.NoResponse => ZIO.unit
      case Message.Direct(nodeAddress, message) =>
        for {
          broadcast     <- broadcast.broadcast(message.size)
          withPiggyback = WithPiggyback(local, message, broadcast)
          chunk         <- ByteCodec[WithPiggyback].toChunk(withPiggyback)
          nodeAddress   <- nodeAddress.socketAddress
          _             <- transport.connect(nodeAddress).use(_.send(chunk))
        } yield ()
      case msg: Message.Batch[Chunk[Byte]] =>
        ZIO.foreach_(msg.first :: msg.second :: msg.rest.toList)(send)
      case msg @ Message.Broadcast(_) =>
        broadcast.add(msg)
      case WithTimeout(message, action, timeout) =>
        send(message) *> action.delay(timeout).flatMap(send).unit
    }

  private def bind =
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
    ZStream
      .mergeAll(2)(
        ZStream
          .fromQueue(messages)
          .collectM {
            case Take.Value(msg: Message.Direct[Chunk[Byte]]) =>
              Take.fromEffect(protocol.onMessage(msg))
          },
        recoverErrors(protocol.produceMessages)
      )
      .mapMPar(10) {
        case Take.Value(msg) =>
          send(msg)
            .catchAll(e => log.error("error during send: " + e))
        case Take.Fail(cause) =>
          log.error("error: ", cause)
        case Take.End => ZIO.unit
      }
      .runDrain
      .fork
}

object Messages {

  final case class WithPiggyback(node: NodeAddress, message: Chunk[Byte], gossip: List[Chunk[Byte]])

  implicit val codec: ByteCodec[WithPiggyback] =
    ByteCodec.fromReadWriter(macroRW[WithPiggyback])

  implicit val chunkRW: ReadWriter[Chunk[Byte]] =
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[Chunk[Byte]](
        ch => ch.toArray,
        arr => Chunk.fromArray(arr)
      )

  def make(
    local: NodeAddress,
    broadcast: Broadcast,
    udpTransport: ConnectionLessTransport.Service
  ) =
    for {
      messageQueue <- Queue
                       .bounded[Take[Error, Message[Chunk[Byte]]]](1000)
                       .toManaged(_.shutdown)
      messages = new Messages(local, messageQueue, broadcast, udpTransport)
      _        <- messages.bind
    } yield messages

}

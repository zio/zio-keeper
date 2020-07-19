package zio.keeper.swim

import upickle.default.{ macroRW, _ }
import zio._
import zio.clock.Clock
import zio.keeper.swim.Message.WithTimeout
import zio.keeper.swim.Messages.WithPiggyback
import zio.keeper.transport.{ Channel, ConnectionLessTransport }
import zio.keeper.{ ByteCodec, Error, NodeAddress, TransportError }
import zio.logging.{ Logging, log }
import zio.stream.{ Take, ZStream }

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
        _.foldM(
          messages.offer(Take.end),
          cause => messages.offer(Take.halt(cause)),
          values =>
            ZIO.foreach_(values) { withPiggyback =>
              val take =
                Take.single(Message.Direct(withPiggyback.node, withPiggyback.conversationId, withPiggyback.message))

              messages.offer(take) *>
                ZIO.foreach_(withPiggyback.gossip) { chunk =>
                  messages.offer(Take.single(Message.Direct(withPiggyback.node, withPiggyback.conversationId, chunk)))
                }
            }
        )
      }
      .unit

  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging, Nothing, Take[E, A]] =
    stream.either.mapM(
      _.fold(
        e => log.error("error during sending", Cause.fail(e)).as(Take.halt(Cause.fail(e))),
        v => ZIO.succeedNow(Take.single(v))
      )
    )

  /**
   * Sends message to target.
   */
  def send(msg: Message[Chunk[Byte]]): ZIO[Clock with Logging, Error, Unit] =
    msg match {
      case Message.NoResponse => ZIO.unit
      case Message.Direct(nodeAddress, conversationId, message) =>
        for {
          broadcast     <- broadcast.broadcast(message.size)
          withPiggyback = WithPiggyback(local, conversationId, message, broadcast)
          chunk         <- ByteCodec[WithPiggyback].toChunk(withPiggyback)
          nodeAddress   <- nodeAddress.socketAddress
          _             <- transport.connect(nodeAddress).use(_.send(chunk))
        } yield ()
      case msg: Message.Batch[Chunk[Byte]] => {
        val (broadcast, rest) =
          (msg.first :: msg.second :: msg.rest.toList).partition(_.isInstanceOf[Message.Broadcast[_]])
        ZIO.foreach_(broadcast)(send) *>
          ZIO.foreach_(rest)(send)
      }
      case msg @ Message.Broadcast(_) =>
        broadcast.add(msg)
      case WithTimeout(message, action, timeout) =>
        send(message) *> action.delay(timeout).flatMap(send).unit
    }

  private def bind: ZManaged[Logging, TransportError, Unit] =
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

  def process(protocol: Protocol[Chunk[Byte]]): ZIO[Clock with Logging, Nothing, Fiber.Runtime[Nothing, Unit]] = {
    def processTake(take: Take[Error, Message[Chunk[Byte]]]) =
      take.foldM(
        ZIO.unit,
        log.error("error: ", _),
        ZIO.foreach_(_)(send(_).catchAll(e => log.error("error during send: " + e)))
      )
    ZStream
      .fromQueue(messages)
      .collectM {
        case Take(Exit.Success(msgs)) =>
          ZIO.foreach(msgs) {
            case msg: Message.Direct[Chunk[Byte]] =>
              Take.fromEffect(protocol.onMessage(msg))
            case _ =>
              ZIO.dieMessage("Something went horribly wrong.")
          }
      }
      .mapMPar(10) { msgs =>
        ZIO.foreach_(msgs)(processTake)
      }
      .runDrain
      .fork *>
      recoverErrors(protocol.produceMessages)
        .mapMPar(10)(processTake)
        .runDrain
        .fork
  }
}

object Messages {

  final case class WithPiggyback(
    node: NodeAddress,
    conversationId: Long,
    message: Chunk[Byte],
    gossip: List[Chunk[Byte]]
  )

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
  ): ZManaged[Logging, TransportError, Messages] =
    for {
      messageQueue <- Queue
                       .bounded[Take[Error, Message[Chunk[Byte]]]](1000)
                       .toManaged(_.shutdown)
      messages = new Messages(local, messageQueue, broadcast, udpTransport)
      _        <- messages.bind
    } yield messages

}

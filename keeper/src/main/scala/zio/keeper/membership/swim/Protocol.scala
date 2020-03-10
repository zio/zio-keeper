package zio.keeper.membership.swim

import zio.keeper.{Error, TaggedCodec}
import zio.logging.Logging
import zio.stream.ZStream
import zio.{Chunk, ZIO}

/**
 * Protocol represents message flow.
 * @tparam M - type of messages handles by this protocol
 */
trait Protocol[M] {
  self =>

  /**
   * Converts this protocol to Chunk[Byte] protocol. This helps with composing multi protocols together.
   *
   * @param codec - TaggedCodec that handles serialization to Chunk[Byte]
   * @return - Protocol that operates on Chunk[Byte]
   */
  final def binary(implicit codec: TaggedCodec[M]): Protocol[Chunk[Byte]] =
    new Protocol[Chunk[Byte]] {

      override val onMessage: Message.Direct[Chunk[Byte]] => ZIO[Any, Error, Option[Message[Chunk[Byte]]]] =
        msg =>
          TaggedCodec
            .read[M](msg.message)
            .flatMap(decoded => self.onMessage(Message.Direct(msg.nodeId, decoded)))
            .flatMap {
              case Some(res) => res.transformM(TaggedCodec.write[M]).map(Some(_))
              case _                 => ZIO.succeed(None)
            }

      override val produceMessages: ZStream[Any, Error, Message[Chunk[Byte]]] =
        self.produceMessages.mapM (_.transformM(TaggedCodec.write[M]))
    }

  /**
   * Composes two protocols together.
   */
  final def compose(other: Protocol[M]): Protocol[M] = new Protocol[M] {

    override def onMessage: Message.Direct[M] => ZIO[Any, Error, Option[Message[M]]] =
      msg =>
        self
          .onMessage(msg)
          .orElse(other.onMessage(msg))

    override def produceMessages: ZStream[Any, Error, Message[M]] =
      self.produceMessages
        .merge(other.produceMessages)
  }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging[String], Error, Protocol[M]] =
    ZIO.access[Logging[String]] { env =>
      new Protocol[M] {
        override def onMessage: Message.Direct[M] => ZIO[Any, Error, Option[Message[M]]] =
          msg =>
            env.logging.info("Receive [" + msg + "]") *>
              self.onMessage(msg)

        override def produceMessages: ZStream[Any, Error, Message[M]] =
          self.produceMessages.tap { msg =>
              env.logging.info("Sending [" + msg + "]")
          }
      }
    }

  /**
   * Handler for incomming messages.
   */
  def onMessage: Message.Direct[M] => ZIO[Any, Error, Option[Message[M]]]

  /**
   * Stream of outgoing messages.
   */
  def produceMessages: zio.stream.ZStream[Any, Error, Message[M]]

}

object Protocol {

  class ProtocolBuilder[M] {

    def apply[R](
                  in: Message.Direct[M] => ZIO[R, Error, Option[Message[M]]],
                  out: zio.stream.ZStream[R, Error, Message[M]]
    ): ZIO[R, Error, Protocol[M]] =
      ZIO.access[R](
        env =>
          new Protocol[M] {

            override val onMessage: Message.Direct[M] => ZIO[Any, Error, Option[Message[M]]] =
              msg => in(msg).provide(env)

            override val produceMessages: ZStream[Any, Error, Message[M]] =
              out.provide(env)
          }
      )
  }

  def apply[M]: ProtocolBuilder[M] =
    new ProtocolBuilder[M]

}

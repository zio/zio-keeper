package zio.keeper.membership.swim

import zio.keeper.{Error, TaggedCodec}
import zio.logging.Logging
import zio.stream.ZStream
import zio.{Chunk, ZIO}

/**
 * Protocol represents message flow.
 * @tparam A - Address type of sender and receiver
 * @tparam M - type of messages handles by this protocol
 */
trait Protocol[A, M] {
  self =>

  /**
   * Converts this protocol to Chunk[Byte] protocol. This helps with composing multi protocols together.
   *
   * @param codec - TaggedCodec that handles serialization to Chunk[Byte]
   * @return - Protocol that operates on Chunk[Byte]
   */
  final def binary(implicit codec: TaggedCodec[M]): Protocol[A, Chunk[Byte]] =
    new Protocol[A, Chunk[Byte]] {

      override val onMessage: (A, Chunk[Byte]) => ZIO[Any, Error, Message[Chunk[Byte]]] =
        (sender, payload) =>
          TaggedCodec
            .read[M](payload)
            .flatMap(m => self.onMessage(sender, m))
            .flatMap {
              case Message.Direct(addr, msg) => TaggedCodec.write(msg).map(chunk => Message.Direct(addr, chunk))
              case _                 => ZIO.succeed[Message[Chunk[Byte]]](Message.Empty)
            }

      override val produceMessages: ZStream[Any, Error, Message[Chunk[Byte]]] =
        self.produceMessages.mapM {
          case Message.Direct(recipient, msg) =>
            TaggedCodec
              .write[M](msg)
              .map(Message.Direct(recipient, _))
        }
    }

  /**
   * Composes two protocols together.
   */
  final def compose(other: Protocol[A, M]): Protocol[A, M] = new Protocol[A, M] {

    override def onMessage: (A, M) => ZIO[Any, Error, Message[M]] =
      (sender, msg) =>
        self
          .onMessage(sender, msg)
          .orElse(other.onMessage(sender, msg))

    override def produceMessages: ZStream[Any, Error, Message[M]] =
      self.produceMessages
        .merge(other.produceMessages)
  }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging[String], Error, Protocol[A, M]] =
    ZIO.access[Logging[String]] { env =>
      new Protocol[A, M] {
        override def onMessage: (A, M) => ZIO[Any, Error, Message[M]] =
          (sender, msg) =>
            env.logging.info("Receive [" + msg + "] from: " + sender) *>
              self.onMessage(sender, msg)

        override def produceMessages: ZStream[Any, Error, Message[M]] =
          self.produceMessages.tap {
            case Message.Direct(to, msg) =>
              env.logging.info("Sending [" + msg + "] to: " + to)
          }
      }
    }

  /**
   * Handler for incomming messages.
   */
  def onMessage: (A, M) => ZIO[Any, Error, Message[M]]

  /**
   * Stream of outgoing messages.
   */
  def produceMessages: zio.stream.ZStream[Any, Error, Message[M]]

}

object Protocol {

  class ProtocolBuilder[A, M] {

    def apply[R](
                  in: (A, M) => ZIO[R, Error, Message[M]],
                  out: zio.stream.ZStream[R, Error, Message[M]]
    ): ZIO[R, Error, Protocol[A, M]] =
      ZIO.access[R](
        env =>
          new Protocol[A, M] {

            override val onMessage: (A, M) => ZIO[Any, Error, Message[M]] =
              (sender, payload) => in(sender, payload).provide(env)

            override val produceMessages: ZStream[Any, Error, Message[M]] =
              out.provide(env)
          }
      )
  }

  def apply[A, M]: ProtocolBuilder[A, M] =
    new ProtocolBuilder[A, M]

}

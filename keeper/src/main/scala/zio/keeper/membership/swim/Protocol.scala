package zio.keeper.membership.swim

import zio.keeper.{ Error, TaggedCodec }
import zio.logging.Logging
import zio.stream.ZStream
import zio.{ Chunk, Queue, ZIO }

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

      override val onMessage: (A, Chunk[Byte]) => ZIO[Any, Error, Option[(A, Chunk[Byte])]] =
        (sender, payload) =>
          TaggedCodec
            .read[M](payload)
            .flatMap(m => self.onMessage(sender, m))
            .flatMap {
              case Some((addr, msg)) => TaggedCodec.write(msg).map(chunk => Some((addr, chunk)))
              case _                 => ZIO.succeed[Option[(A, Chunk[Byte])]](None)
            }

      override val produceMessages: ZStream[Any, Error, (A, Chunk[Byte])] =
        self.produceMessages.mapM {
          case (recipient, msg) =>
            TaggedCodec
              .write[M](msg)
              .map((recipient, _))
        }
    }

  /**
   * Composes two protocols together.
   */
  final def compose(other: Protocol[A, M]): Protocol[A, M] = new Protocol[A, M] {

    override def onMessage: (A, M) => ZIO[Any, Error, Option[(A, M)]] =
      (sender, msg) =>
        self
          .onMessage(sender, msg)
          .orElse(other.onMessage(sender, msg))

    override def produceMessages: ZStream[Any, Error, (A, M)] =
      self.produceMessages
        .merge(other.produceMessages)
  }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging[String], Error, Protocol[A, M]] =
    ZIO.access[Logging[String]] { env =>
      new Protocol[A, M] {
        override def onMessage: (A, M) => ZIO[Any, Error, Option[(A, M)]] =
          (sender, msg) =>
            env.logging.info("Receive [" + msg + "] from: " + sender) *>
              self.onMessage(sender, msg)

        override def produceMessages: ZStream[Any, Error, (A, M)] =
          self.produceMessages.tap {
            case (to, msg) =>
              env.logging.info("Sending [" + msg + "] to: " + to)
          }
      }
    }

  /**
   * Handler for incomming messages.
   */
  def onMessage: (A, M) => ZIO[Any, Error, Option[(A, M)]]

  /**
   * Stream of outgoing messages.
   */
  def produceMessages: zio.stream.ZStream[Any, Error, (A, M)]

}

object Protocol {

  class ProtocolBuilder[A, M] {

    def apply[R](
      in: (A, M) => ZIO[R, Error, Option[(A, M)]],
      out: zio.stream.ZStream[R, Error, (A, M)]
    ): ZIO[R, Error, Protocol[A, M]] =
      ZIO.access[R](
        env =>
          new Protocol[A, M] {

            override val onMessage: (A, M) => ZIO[Any, Error, Option[(A, M)]] =
              (sender, payload) => in(sender, payload).provide(env)

            override val produceMessages: ZStream[Any, Error, (A, M)] =
              out.provide(env)
          }
      )
  }

  def apply[A, M]: ProtocolBuilder[A, M] =
    new ProtocolBuilder[A, M]

}

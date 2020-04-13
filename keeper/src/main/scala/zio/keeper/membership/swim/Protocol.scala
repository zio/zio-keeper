package zio.keeper.membership.swim

import zio.keeper.Error
import zio.keeper.membership.TaggedCodec
import zio.logging.Logging.Logging
import zio.logging._
import zio.stream.ZStream
import zio.{ Chunk, IO, ZIO }
import zio.stream._

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

      override val onMessage: Message.Direct[Chunk[Byte]] => ZIO[Any, Error, Message[Chunk[Byte]]] =
        msg =>
          TaggedCodec
            .read[M](msg.message)
            .flatMap(decoded => self.onMessage(Message.Direct(msg.node, decoded)))
            .flatMap(_.transformM(TaggedCodec.write[M]))

      override val produceMessages: Stream[Error, Message[Chunk[Byte]]] =
        self.produceMessages.mapM(_.transformM(TaggedCodec.write[M]))
    }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging, Error, Protocol[M]] =
    ZIO.access[Logging] { env =>
      new Protocol[M] {
        override def onMessage: Message.Direct[M] => IO[Error, Message[M]] =
          msg =>
            env.get.logger.log(LogLevel.Trace)("Receive [" + msg + "]") *>
              self
                .onMessage(msg)
                .tap(msg => env.get.logger.log(LogLevel.Trace)("Replied with [" + msg + "]"))

        override val produceMessages: Stream[Error, Message[M]] =
          self.produceMessages.tap { msg =>
            env.get.logger.log(LogLevel.Trace)("Sending [" + msg + "]")
          }
      }
    }

  /**
   * Handler for incomming messages.
   */
  def onMessage: Message.Direct[M] => IO[Error, Message[M]]

  /**
   * Stream of outgoing messages.
   */
  val produceMessages: zio.stream.Stream[Error, Message[M]]

}

object Protocol {

  def compose[A](first: Protocol[A], second: Protocol[A], rest: Protocol[A]*): Protocol[A] =
    new Protocol[A] {

      override val onMessage: Message.Direct[A] => IO[Error, Message[A]] =
        msg => (second :: rest.toList).foldLeft(first.onMessage(msg))((acc, a) => acc.orElse(a.onMessage(msg)))

      override val produceMessages: Stream[Error, Message[A]] =
        ZStream.mergeAllUnbounded()(
          (first.produceMessages :: second.produceMessages :: rest.map(_.produceMessages).toList): _*
        )

    }

  class ProtocolBuilder[M] {

    def make[R](
      in: Message.Direct[M] => ZIO[R, Error, Message[M]],
      out: zio.stream.ZStream[R, Error, Message[M]]
    ): ZIO[R, Error, Protocol[M]] =
      ZIO.access[R](
        env =>
          new Protocol[M] {

            override val onMessage: Message.Direct[M] => IO[Error, Message[M]] =
              msg => in(msg).provide(env)

            override val produceMessages: Stream[Error, Message[M]] =
              out.provide(env)
          }
      )
  }

  def apply[M]: ProtocolBuilder[M] =
    new ProtocolBuilder[M]

}

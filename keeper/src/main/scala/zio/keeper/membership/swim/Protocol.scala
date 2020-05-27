package zio.keeper.membership.swim

import zio.keeper.{ ByteCodec, Error }
import zio.logging.{ Logging, _ }
import zio.stream.{ ZStream, _ }
import zio.{ Chunk, IO, ZIO }

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
  final def binary(implicit codec: ByteCodec[M]): Protocol[Chunk[Byte]] =
    new Protocol[Chunk[Byte]] {

      override val onMessage: Message.Direct[Chunk[Byte]] => ZIO[Any, Error, Message[Chunk[Byte]]] =
        msg =>
          ByteCodec
            .decode[M](msg.message)
            .flatMap(decoded => self.onMessage(msg.copy(message = decoded)))
            .flatMap(_.transformM(ByteCodec.encode[M]))

      override val produceMessages: Stream[Error, Message[Chunk[Byte]]] =
        self.produceMessages.mapM(_.transformM(ByteCodec.encode[M]))
    }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging, Error, Protocol[M]] =
    ZIO.access[Logging] { env =>
      new Protocol[M] {
        override def onMessage: Message.Direct[M] => IO[Error, Message[M]] =
          msg =>
            env.get.log(LogLevel.Trace)("Receive [" + msg + "]") *>
              self
                .onMessage(msg)
                .tap(msg => env.get.log(LogLevel.Trace)("Replied with [" + msg + "]"))

        override val produceMessages: Stream[Error, Message[M]] =
          self.produceMessages.tap { msg =>
            env.get.log(LogLevel.Trace)("Sending [" + msg + "]")
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

    def make[R, R1](
      in: Message.Direct[M] => ZIO[R, Error, Message[M]],
      out: zio.stream.ZStream[R1, Error, Message[M]]
    ): ZIO[R with R1, Error, Protocol[M]] =
      ZIO.access[R with R1](
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

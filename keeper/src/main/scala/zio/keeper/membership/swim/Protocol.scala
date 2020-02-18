package zio.keeper.membership.swim

import zio.keeper.{ Error, TaggedCodec }
import zio.logging.Logging
import zio.stream.ZStream
import zio.{ Chunk, ZIO }

trait Protocol[A, M] {
  self =>

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

  def onMessage: (A, M) => ZIO[Any, Error, Option[(A, M)]]

  def produceMessages: zio.stream.ZStream[Any, Error, (A, M)]

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

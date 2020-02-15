package zio.keeper.membership.swim

import zio.keeper.{ Error, TaggedCodec }
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

}

object Protocol {

  def apply[A, M: TaggedCodec](
    in: (A, M) => ZIO[Any, Error, Option[(A, M)]],
    out: zio.stream.ZStream[Any, Error, (A, M)]
  ): Protocol[A, Chunk[Byte]] = new Protocol[A, Chunk[Byte]] {

    override def onMessage: (A, Chunk[Byte]) => ZIO[Any, Error, Option[(A, Chunk[Byte])]] =
      (sender, payload) =>
        TaggedCodec
          .read[M](payload)
          .flatMap(m => in(sender, m))
          .flatMap {
            case Some((addr, msg)) => TaggedCodec.write(msg).map(chunk => Some((addr, chunk)))
            case _                 => ZIO.succeed[Option[(A, Chunk[Byte])]](None)
          }

    override def produceMessages: ZStream[Any, Error, (A, Chunk[Byte])] =
      out.mapM {
        case (recipient, msg) =>
          TaggedCodec
            .write[M](msg)
            .map((recipient, _))
      }
  }
}

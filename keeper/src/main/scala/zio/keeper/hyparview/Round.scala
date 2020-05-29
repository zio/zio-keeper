package zio.keeper.hyparview.plumtree

import zio._
import zio.keeper.ByteCodec
import zio.keeper.encoding._
import zio.keeper.SerializationError.DeserializationTypeError

sealed abstract case class Round(value: Int) {

  lazy val inc: Round =
    new Round(value + 1) {}
}

object Round {

  implicit val codec: ByteCodec[Round] =
    ByteCodec.instance(
      chunk =>
        byteArrayToInt(chunk.toArray).flatMap {
          case x if x >= 0 => ZIO.succeed(new Round(x) {})
          case x           => ZIO.fail(DeserializationTypeError(s"Invalid range for round $x"))
        }
    )(
      round => ZIO.succeed(Chunk.fromArray(intToByteArray(round.value)))
    )

  implicit val ordering: Ordering[Round] = Ordering.by(_.value)

  val zero: Round = new Round(0) {}
}

package zio.keeper.membership.hyparview.plumtree

import zio._
import zio.keeper.ByteCodec
import zio.keeper.SerializationError.DeserializationTypeError

sealed abstract case class Round(value: Int) {

  def inc: Round =
    new Round(value + 1) {}
}

object Round {

  implicit val codec: ByteCodec[Round] =
    ByteCodec.instance(
      chunk =>
        BigInt(chunk.toArray).intValue match {
          case x if x >= 0 => ZIO.succeed(new Round(x) {})
          case x           => ZIO.fail(DeserializationTypeError(s"Invalid range for round $x"))
        }
    )(
      round => ZIO.succeed(Chunk.fromArray(BigInt(round.value).toByteArray))
    )

  val zero: Round =
    new Round(0) {}

}

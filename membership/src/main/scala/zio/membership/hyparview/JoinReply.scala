package zio.membership.hyparview

import upickle.default._
import zio.keeper.membership.ByteCodec

final case class JoinReply[T](
  remote: T
) extends AnyVal

object JoinReply {

  implicit def codec[T: ReadWriter]: ByteCodec[JoinReply[T]] =
    ByteCodec.fromReadWriter(macroRW[JoinReply[T]])

}

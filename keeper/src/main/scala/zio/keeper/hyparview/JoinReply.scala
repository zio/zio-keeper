package zio.keeper.hyparview

import upickle.default._
import zio.keeper.{ ByteCodec, NodeAddress }

final case class JoinReply(
  remote: NodeAddress
) extends AnyVal

object JoinReply {

  implicit val codec: ByteCodec[JoinReply] =
    ByteCodec.fromReadWriter(macroRW[JoinReply])

}

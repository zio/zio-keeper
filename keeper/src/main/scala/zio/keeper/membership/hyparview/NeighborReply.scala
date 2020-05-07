package zio.keeper.membership.hyparview

import upickle.default._
import zio.keeper.{ ByteCodec, TaggedCodec }

sealed abstract class NeighborReply

object NeighborReply {

  implicit val tagged: TaggedCodec[NeighborReply] =
    TaggedCodec.instance(
      {
        case _: Reject.type => 20
        case _: Accept.type => 21
      }, {
        case 20 => ByteCodec[Reject.type].asInstanceOf[ByteCodec[NeighborReply]]
        case 21 => ByteCodec[Accept.type].asInstanceOf[ByteCodec[NeighborReply]]
      }
    )

  case object Reject extends NeighborReply {

    implicit val codec: ByteCodec[Reject.type] =
      ByteCodec.fromReadWriter(macroRW[Reject.type])
  }

  case object Accept extends NeighborReply {

    implicit val codec: ByteCodec[Accept.type] =
      ByteCodec.fromReadWriter(macroRW[Accept.type])
  }
}

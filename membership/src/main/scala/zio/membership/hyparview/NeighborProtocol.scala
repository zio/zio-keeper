package zio.membership.hyparview

import zio.membership.ByteCodec
import upickle.default._

sealed private[hyparview] trait NeighborProtocol

private[hyparview] object NeighborProtocol {

  implicit val tagged: Tagged[NeighborProtocol] =
    Tagged.instance(
      {
        case _: Reject.type => 20
        case _: Accept.type => 21
      }, {
        case 20 => ByteCodec[Reject.type].asInstanceOf[ByteCodec[NeighborProtocol]]
        case 21 => ByteCodec[Accept.type].asInstanceOf[ByteCodec[NeighborProtocol]]
      }
    )

  case object Reject extends NeighborProtocol {

    implicit val codec: ByteCodec[Reject.type] =
      ByteCodec.fromReadWriter(macroRW[Reject.type])
  }

  case object Accept extends NeighborProtocol {

    implicit val codec: ByteCodec[Accept.type] =
      ByteCodec.fromReadWriter(macroRW[Accept.type])
  }

}

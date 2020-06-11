package zio.keeper.hyparview

import upickle.default._
import zio.keeper.ByteCodec

sealed abstract class NeighborReply

object NeighborReply {

  implicit val byteCodec: ByteCodec[NeighborReply] =
    ByteCodec.tagged[NeighborReply][
      Reject.type,
      Accept.type
    ]

  case object Reject extends NeighborReply {

    implicit val codec: ByteCodec[Reject.type] =
      ByteCodec.fromReadWriter(macroRW[Reject.type])
  }

  case object Accept extends NeighborReply {

    implicit val codec: ByteCodec[Accept.type] =
      ByteCodec.fromReadWriter(macroRW[Accept.type])
  }
}

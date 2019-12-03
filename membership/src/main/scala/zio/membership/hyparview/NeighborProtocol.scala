package zio.membership.hyparview

import zio.membership.ByteCodec

sealed trait NeighborProtocol
object NeighborProtocol {
  implicit val codec: ByteCodec[NeighborProtocol] = null

  case object Reject extends NeighborProtocol {
    implicit val codec: ByteCodec[Reject.type] = null
  }

  case object Accept extends NeighborProtocol {
    implicit val codec: ByteCodec[Accept.type] = null
  }

}

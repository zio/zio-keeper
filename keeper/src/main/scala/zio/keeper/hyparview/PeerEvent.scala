package zio.keeper.hyparview

import zio.keeper.NodeAddress

sealed abstract class PeerEvent

object PeerEvent {

  final case class NeighborUp(node: NodeAddress)   extends PeerEvent
  final case class NeighborDown(node: NodeAddress) extends PeerEvent

}

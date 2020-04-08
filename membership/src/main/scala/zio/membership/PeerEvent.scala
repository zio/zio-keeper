package zio.membership

sealed abstract class PeerEvent[T]

object PeerEvent {

  final case class NeighborUp[T](node: T)   extends PeerEvent[T]
  final case class NeighborDown[T](node: T) extends PeerEvent[T]

}

package zio.keeper.hyparview

import zio.keeper.NodeAddress

sealed abstract class ViewEvent

object ViewEvent {

  final case class AddedToActiveView(node: NodeAddress)                             extends ViewEvent
  final case class AddedToPassiveView(node: NodeAddress)                            extends ViewEvent
  final case class PeerMessageReceived(node: NodeAddress, msg: Message.PeerMessage) extends ViewEvent
  final case class RemovedFromActiveView(node: NodeAddress)                         extends ViewEvent
  final case class RemovedFromPassiveView(node: NodeAddress)                        extends ViewEvent
  final case class UnhandledMessage(to: NodeAddress, msg: Message)                  extends ViewEvent
}

package zio.keeper.membership

import zio.keeper.membership.swim.Nodes.NodeState

sealed trait MembershipEvent

object MembershipEvent {
  final case class Join(id: NodeAddress)                                                  extends MembershipEvent
  final case class NodeStateChanged(id: NodeAddress, old: NodeState, newState: NodeState) extends MembershipEvent
}

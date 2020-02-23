package zio.keeper.membership

import zio.keeper.membership.swim.NodeId
import zio.keeper.membership.swim.Nodes.NodeState

sealed trait MembershipEvent

object MembershipEvent {

  final case class Join(id: NodeId) extends MembershipEvent

  final case class NodeStateChanged(id: NodeId, previous: NodeState, current: NodeState) extends MembershipEvent

}

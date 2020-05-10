package zio.keeper.membership

import zio.keeper.NodeAddress

sealed trait MembershipEvent

object MembershipEvent {
  final case class Join(id: NodeAddress)  extends MembershipEvent
  final case class Leave(id: NodeAddress) extends MembershipEvent
}

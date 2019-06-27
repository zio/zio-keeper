package zio.keeper

sealed trait MembershipEvent

object MembershipEvent {
  final case class Join(member: NodeId)        extends MembershipEvent
  final case class Leave(member: NodeId)       extends MembershipEvent
  final case class Unreachable(member: NodeId) extends MembershipEvent
}

package zio.keeper.membership

sealed trait MembershipEvent

object MembershipEvent {

  final case class Join(member: NodeAddress) extends MembershipEvent

  final case class Leave(member: NodeAddress) extends MembershipEvent

  final case class Unreachable(member: NodeAddress) extends MembershipEvent

}

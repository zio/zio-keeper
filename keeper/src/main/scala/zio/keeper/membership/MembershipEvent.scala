package zio.keeper.membership

sealed trait MembershipEvent

object MembershipEvent {

  final case class Join(member: Member) extends MembershipEvent

  final case class Leave(member: Member) extends MembershipEvent

  final case class Unreachable(member: Member) extends MembershipEvent

}

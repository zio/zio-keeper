package zio.keeper.membership

sealed trait MembershipEvent[A]

object MembershipEvent {

  final case class Join[A](member: Member[A]) extends MembershipEvent[A]

  final case class Leave[A](member: Member[A]) extends MembershipEvent[A]

  final case class Unreachable[A](member: Member[A]) extends MembershipEvent[A]

}

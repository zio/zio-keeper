package scalaz.ziokeeper

sealed trait Membership

object Membership {
  final case class Join(member: Member)        extends Membership
  final case class Leave(member: Member)       extends Membership
  final case class Unreachable(member: Member) extends Membership
}

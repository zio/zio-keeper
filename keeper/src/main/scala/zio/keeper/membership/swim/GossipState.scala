package zio.keeper.membership.swim

import upickle.default.{macroRW, _}
import zio.keeper.membership.swim
import zio.keeper.membership.swim.GossipState.StateDiff


case class GossipState[A](members: Vector[A]) extends AnyVal {

  def addMember(member: A) =
    copy(members = this.members :+ member)

  def diff(other: GossipState[A]): StateDiff[A] =
    StateDiff(
      this.members.diff(other.members),
      other.members.diff(this.members)
    )

  def merge(other: GossipState[A]) =
    copy(members = this.members ++ other.members)

  def removeMember(member: A) =
    copy(members = this.members.filterNot(_ == member))

  override def toString: String = s"GossipState[${members.mkString(",")}] "
}

object GossipState {
  def Empty[A] = swim.GossipState(Vector.empty[A])

  final case class StateDiff[A](local: Vector[A], remote: Vector[A])
  implicit def gossipStateRw[A: ReadWriter] = macroRW[GossipState[A]]
}

sealed trait NodeState

object NodeState {
  case object New     extends NodeState
  case object Healthy extends NodeState
}

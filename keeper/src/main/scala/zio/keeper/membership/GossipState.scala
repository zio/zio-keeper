package zio.keeper.membership

import scala.collection.immutable.SortedSet

case class GossipState(members: SortedSet[Member]) extends AnyVal {
  import zio.keeper.membership.GossipState.StateDiff

  def addMember(member: Member) =
    copy(members = this.members + member)

  def diff(other: GossipState): StateDiff =
    StateDiff(
      this.members.diff(other.members),
      other.members.diff(this.members)
    )

  def merge(other: GossipState) =
    copy(members = this.members ++ other.members)

  def removeMember(member: Member) =
    copy(members = this.members - member)

  override def toString: String = s"GossipState[${members.mkString(",")}] "
}

object GossipState {
  val Empty = GossipState(SortedSet())

  final case class StateDiff(local: SortedSet[Member], remote: SortedSet[Member])
}

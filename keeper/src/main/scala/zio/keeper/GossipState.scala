package zio.keeper

import zio.keeper.GossipState.StateDiff

import scala.collection.immutable.SortedSet

case class GossipState(members: SortedSet[Member]) extends AnyVal {

  def merge(other: GossipState) =
    copy(members = this.members ++ other.members)

  def addMember(member: Member) =
    copy(members = this.members + member)

  def removeMember(member: Member) =
    copy(members = this.members - member)

  def diff(other: GossipState): StateDiff =
    StateDiff(
      this.members.diff(other.members),
      other.members.diff(this.members)
    )
}

object GossipState {
  val Empty = GossipState(SortedSet())
  final case class StateDiff(local: SortedSet[Member], remote: SortedSet[Member])
}

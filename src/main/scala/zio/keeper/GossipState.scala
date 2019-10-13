package zio.keeper

import zio.keeper.GossipState.StateDiff

case class GossipState(members: Set[Member]) extends AnyVal {

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
  val Empty = GossipState(Set.empty)
  final case class StateDiff(local: Set[Member], remote: Set[Member])
}

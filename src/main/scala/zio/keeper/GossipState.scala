package zio.keeper

case class GossipState(members: Set[Member]) extends AnyVal {

  def merge(other: GossipState) =
    copy(members = this.members ++ other.members)
  def addMember(member: Member) = copy(members = this.members + member)
}

object GossipState {
  val Empty = GossipState(Set.empty)
}

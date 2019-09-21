package zio.keeper

case class GossipState(members: Set[Member]) extends AnyVal {
  def merge(other: GossipState) = copy(members = this.members ++ other.members)
}
object GossipState {
  val Empty = GossipState(Set.empty)
}

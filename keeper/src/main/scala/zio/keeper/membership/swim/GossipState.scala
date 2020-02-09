package zio.keeper.membership.swim

import upickle.default.{ macroRW, _ }
import zio.ZIO
import zio.keeper.Error
import zio.keeper.membership.swim.GossipState.StateDiff
import zio.keeper.membership.{ Member, NodeId, swim }

import scala.collection.immutable.SortedSet

trait Gossip[A] {
  def gossip: Gossip.Service[Any, A]
}

object Gossip {

  trait Service[R, A] {
    def state: ZIO[R, Error, GossipState[A]]
    def ack(id: Long): ZIO[R, Error, Unit]
    def updateState(newState: GossipState[A]): ZIO[R, Error, Unit]
    def nodeState(nodeId: NodeId): ZIO[R, Error, NodeState]
    def modifyNodeState(nodeId: NodeId, mod: NodeState => NodeState): ZIO[R, Error, Unit]
  }
}

case class GossipState[A](members: SortedSet[Member[A]]) extends AnyVal {

  def addMember(member: Member[A]) =
    copy(members = this.members + member)

  def diff(other: GossipState[A]): StateDiff[A] =
    StateDiff(
      this.members.diff(other.members),
      other.members.diff(this.members)
    )

  def merge(other: GossipState[A]) =
    copy(members = this.members ++ other.members)

  def removeMember(member: Member[A]) =
    copy(members = this.members - member)

  override def toString: String = s"GossipState[${members.mkString(",")}] "
}

object GossipState {
  def Empty[A] = swim.GossipState(SortedSet.empty[Member[A]])

  final case class StateDiff[A](local: SortedSet[Member[A]], remote: SortedSet[Member[A]])
  implicit def gossipStateRw[A: ReadWriter] = macroRW[GossipState[A]]
}

sealed trait NodeState

object NodeState {
  case object New     extends NodeState
  case object Healthy extends NodeState
}

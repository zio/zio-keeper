package zio.keeper.membership

import zio.ZIO
import zio.keeper.Error
import zio.keeper.membership.swim.NodeId
import zio.stream.ZStream

trait Membership[B] {
  def membership: Membership.Service[Any, B]
}

object Membership {

  trait Service[R, B] {

    def events: ZStream[R, Error, MembershipEvent]

    def localMember: NodeId

    def nodes: ZIO[R, Nothing, List[NodeId]]

    def receive: ZStream[R, Error, (NodeId, B)]

    def send(data: B, receipt: NodeId): ZIO[R, Error, Unit]
  }

}

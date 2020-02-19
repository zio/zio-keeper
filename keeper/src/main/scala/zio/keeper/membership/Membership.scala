package zio.keeper.membership

import zio.ZIO
import zio.keeper.Error
import zio.stream.ZStream

trait Membership[B] {
  def membership: Membership.Service[Any, B]
}

object Membership {

  trait Service[R, B] {

    def events: ZStream[R, Error, MembershipEvent]

    def localMember: NodeAddress

    def nodes: ZIO[R, Nothing, List[NodeAddress]]

    def receive: ZStream[R, Error, (NodeAddress, B)]

    def send(data: B, receipt: NodeAddress): ZIO[R, Error, Unit]
  }

}

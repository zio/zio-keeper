package zio.keeper.membership

import zio.stream.Stream
import zio.{ Has, IO, UIO }

object Membership {
  type Membership[A] = Has[Membership.Service[A]]

  trait Service[A] {
    def events: Stream[zio.keeper.Error, MembershipEvent]
    def localMember: NodeAddress
    def nodes: UIO[List[NodeAddress]]
    def receive: Stream[zio.keeper.Error, (NodeAddress, A)]
    def send(data: A, receipt: NodeAddress): IO[zio.keeper.Error, Unit]
  }
}

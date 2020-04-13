package zio.keeper.membership

import zio.stream.Stream
import zio.{ IO, UIO }

object Membership {

  trait Service[A] {
    def broadcast(data: A): IO[zio.keeper.Error, Unit]
    def events: Stream[zio.keeper.Error, MembershipEvent]
    def localMember: NodeAddress
    def nodes: UIO[List[NodeAddress]]
    def receive: Stream[zio.keeper.Error, (NodeAddress, A)]
    def send(data: A, receipt: NodeAddress): IO[zio.keeper.Error, Unit]
  }
}

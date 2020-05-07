package zio.keeper.membership

import zio.keeper.{ NodeAddress, SendError }
import zio.stream.Stream
import zio.{ IO, UIO }

object Membership {

  trait Service[A] {
    def broadcast(data: A): IO[zio.keeper.Error, Unit]
    // def events: Stream[Nothing, MembershipEvent]
    def localMember: UIO[NodeAddress]
    def nodes: UIO[Set[NodeAddress]]
    def receive: Stream[Nothing, (NodeAddress, A)]
    def send(data: A, receipt: NodeAddress): IO[SendError, Unit]
  }
}

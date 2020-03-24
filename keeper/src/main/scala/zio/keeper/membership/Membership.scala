package zio.keeper.membership

import zio.keeper.membership.swim.NodeId
import zio.stream.Stream
import zio.{Has, IO, UIO, ZIO}

object Membership {
  type Membership[A] = Has[Membership.Service[A]]

  trait Service[A] {
    def events: Stream[zio.keeper.Error, MembershipEvent]
    def localMember: NodeId
    def nodes: UIO[List[NodeId]]
    def receive: Stream[zio.keeper.Error, (NodeId, A)]
    def send(data: A, receipt: NodeId): IO[zio.keeper.Error, Unit]
  }
}

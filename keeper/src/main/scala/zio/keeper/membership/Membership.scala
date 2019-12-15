package zio.keeper.membership

import zio.keeper.{ Error, Message }
import zio.stream.ZStream
import zio.{ Chunk, ZIO }

trait Membership {
  def membership: Membership.Service[Any]
}

object Membership {

  trait Service[R] {
    def localMember: ZIO[R, Nothing, Member]

    def nodes: ZIO[R, Nothing, List[NodeId]]

    def events: ZStream[R, Error, MembershipEvent]

    def send(data: Chunk[Byte], receipt: NodeId): ZIO[R, Error, Unit]

    def broadcast(data: Chunk[Byte]): ZIO[R, Error, Unit]

    def receive: ZStream[R, Error, Message]
  }
}

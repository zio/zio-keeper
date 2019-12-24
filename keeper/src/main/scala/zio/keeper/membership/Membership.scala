package zio.keeper.membership

import zio.keeper.{ Error, Message }
import zio.stream.ZStream
import zio.{ Chunk, ZIO }

trait Membership {
  def membership: Membership.Service[Any]
}

object Membership {

  trait Service[R] {

    def broadcast(data: Chunk[Byte]): ZIO[R, Error, Unit]

    val events: ZStream[R, Error, MembershipEvent]

    val localMember: ZIO[R, Nothing, Member]

    val nodes: ZIO[R, Nothing, List[NodeId]]

    val receive: ZStream[R, Error, Message]

    def send(data: Chunk[Byte], receipt: NodeId): ZIO[R, Error, Unit]
  }
}

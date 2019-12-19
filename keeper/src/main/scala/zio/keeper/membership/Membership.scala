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

    //conv
    val events: ZStream[R, Error, MembershipEvent]

    //conv
    val localMember: ZIO[R, Nothing, Member]

    //conv
    val nodes: ZIO[R, Nothing, List[NodeId]]

    //conv
    val receive: ZStream[R, Error, Message]

    def send(data: Chunk[Byte], receipt: NodeId): ZIO[R, Error, Unit]
  }
}

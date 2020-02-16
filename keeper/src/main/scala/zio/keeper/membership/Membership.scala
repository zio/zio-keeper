package zio.keeper.membership

import zio.ZIO
import zio.keeper.Error
import zio.stream.ZStream

trait Membership[A, B] {
  def membership: Membership.Service[Any, A, B]
}

object Membership {

  trait Service[R, A, B] {
//    def broadcast(data: Chunk[Byte]): ZIO[R, Error, Unit]

    def events: ZStream[R, Error, MembershipEvent[A]]

    def localMember: A

    def nodes: ZIO[R, Nothing, List[A]]

    def receive: ZStream[R, Error, (A, B)]

    def send(data: B, receipt: A): ZIO[R, Error, Unit]
  }

}

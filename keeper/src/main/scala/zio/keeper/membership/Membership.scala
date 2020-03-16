package zio.keeper.membership

import zio.{ Chunk, IO, UIO }
import zio.keeper.{ Error, Message }
import zio.stream.Stream

object Membership {

  trait Service {
    def broadcast(data: Chunk[Byte]): IO[Error, Unit]
    def events: Stream[Error, MembershipEvent]
    def localMember: UIO[Member]
    def nodes: UIO[List[NodeId]]
    def receive: Stream[Error, Message]
    def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit]
  }
}

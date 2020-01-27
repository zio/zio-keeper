package zio.keeper

import zio.stream.ZStream
import zio.{ Chunk, ZIO }

package object membership extends Membership.Service[Membership] {

  override def broadcast(messageId: String, data: Chunk[Byte]): ZIO[Membership, Error, Unit] =
    ZIO.accessM[Membership](_.membership.broadcast(messageId, data))

  override def events: ZStream[Membership, Error, MembershipEvent] =
    ZStream.unwrap(ZIO.access[Membership](_.membership.events))

  override def localMember: ZIO[Membership, Nothing, Member] =
    ZIO.accessM[Membership](_.membership.localMember)

  override def nodes: ZIO[Membership, Nothing, List[NodeId]] =
    ZIO.accessM[Membership](_.membership.nodes)

  override def receive: ZStream[Membership, Error, Message] =
    ZStream.unwrap(ZIO.access[Membership](_.membership.receive))

  override def send(messageId: String, data: Chunk[Byte], receipt: NodeId): ZIO[Membership, Error, Unit] =
    ZIO.accessM[Membership](_.membership.send(messageId, data, receipt))
}

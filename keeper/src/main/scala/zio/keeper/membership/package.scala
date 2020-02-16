/*
package zio.keeper

import zio.stream.ZStream
import zio.{ Chunk, ZIO }

package object membership extends Membership.Service[Membership[NodeAddress], NodeAddress] {

  override def broadcast(data: Chunk[Byte]): ZIO[Membership[NodeAddress], Error, Unit] =
    ZIO.accessM[Membership[NodeAddress]](_.membership.broadcast(data))

  override def events: ZStream[Membership[NodeAddress], Error, MembershipEvent[NodeAddress]] =
    ZStream.unwrap(ZIO.access[Membership[NodeAddress]](_.membership.events))

  override def localMember: ZIO[Membership[NodeAddress], Nothing, Member[NodeAddress]] =
    ZIO.accessM[Membership[NodeAddress]](_.membership.localMember)

  override def nodes: ZIO[Membership[NodeAddress], Nothing, List[NodeId]] =
    ZIO.accessM[Membership[NodeAddress]](_.membership.nodes)

  override def receive: ZStream[Membership[NodeAddress], Error, Message] =
    ZStream.unwrap(ZIO.access[Membership[NodeAddress]](_.membership.receive))

  override def send(data: Chunk[Byte], receipt: NodeId): ZIO[Membership[NodeAddress], Error, Unit] =
    ZIO.accessM[Membership[NodeAddress]](_.membership.send(data, receipt))
}
*/

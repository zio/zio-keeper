package zio.keeper

import zio.stream.ZStream
import zio.{ Chunk, Has, ZIO }

package object membership {
  type Membership[A] = Has[Membership.Service[A]]

  def broadcast(data: Chunk[Byte]): ZIO[Membership[Chunk[Byte]], Error, Unit] =
    ZIO.accessM(_.get.broadcast(data))

  //  def events: ZStream[Membership[Chunk[Byte]], Error, MembershipEvent] =
  //    ZStream.accessStream(_.get.events)

  def localMember: ZIO[Membership[Chunk[Byte]], Nothing, NodeAddress] =
    ZIO.accessM(_.get.localMember)

  def nodes: ZIO[Membership[Chunk[Byte]], Nothing, Set[NodeAddress]] =
    ZIO.accessM(_.get.nodes)

  def receive: ZStream[Membership[Chunk[Byte]], Error, (NodeAddress, Chunk[Byte])] =
    ZStream.accessStream(_.get.receive)

  def send(data: Chunk[Byte], receipt: NodeAddress): ZIO[Membership[Chunk[Byte]], Error, Unit] =
    ZIO.accessM(_.get.send(data, receipt))

}

package zio.keeper

import izumi.reflect.Tags.Tag
import zio.stream.ZStream
import zio.{ Has, ZIO }

package object membership {
  type Membership[A] = Has[Membership.Service[A]]

  def broadcast[A: Tag](data: A): ZIO[Membership[A], Error, Unit] =
    ZIO.accessM(_.get.broadcast(data))

  def events[A: Tag]: ZStream[Membership[A], Error, MembershipEvent] =
    ZStream.accessStream(_.get.events)

  def localMember[A: Tag]: ZIO[Membership[A], Nothing, NodeAddress] =
    ZIO.accessM(_.get.localMember)

  def nodes[A: Tag]: ZIO[Membership[A], Nothing, Set[NodeAddress]] =
    ZIO.accessM(_.get.nodes)

  def receive[A: Tag]: ZStream[Membership[A], Error, (NodeAddress, A)] =
    ZStream.accessStream(_.get.receive)

  def send[A: Tag](data: A, receipt: NodeAddress): ZIO[Membership[A], Error, Unit] =
    ZIO.accessM(_.get.send(data, receipt))

}

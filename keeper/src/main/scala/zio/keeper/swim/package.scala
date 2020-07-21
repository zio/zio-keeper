package zio.keeper

import zio.stream.ZStream
import zio.{ Has, Tag, ZIO }

package object swim {
  type ConversationId        = Has[ConversationId.Service]
  type Nodes                 = Has[Nodes.Service]
  type MessageAcknowledge    = Has[MessageAcknowledge.Service]
  type SuspicionTimeout      = Has[SuspicionTimeout.Service]
  type Swim[A]               = Has[Swim.Service[A]]
  type LocalHealthMultiplier = Has[LocalHealthMultiplier.Service]

  def broadcast[A: Tag](data: A): ZIO[Swim[A], Error, Unit] =
    ZIO.accessM(_.get.broadcast(data))

  def events[A: Tag]: ZStream[Swim[A], Error, MembershipEvent] =
    ZStream.accessStream(_.get.events)

  def localMember[A: Tag]: ZIO[Swim[A], Nothing, NodeAddress] =
    ZIO.access(_.get.localMember)

  def nodes[A: Tag]: ZIO[Swim[A], Nothing, Set[NodeAddress]] =
    ZIO.accessM(_.get.nodes)

  def receive[A: Tag]: ZStream[Swim[A], Error, (NodeAddress, A)] =
    ZStream.accessStream(_.get.receive)

  def send[A: Tag](data: A, receipt: NodeAddress): ZIO[Swim[A], Error, Unit] =
    ZIO.accessM(_.get.send(data, receipt))

}

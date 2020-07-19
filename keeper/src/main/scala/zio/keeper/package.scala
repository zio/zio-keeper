package zio

import izumi.reflect.Tag
import zio.stream._

package object keeper extends MembershipProtocol with ConsensusProtocol {
  type Keeper[A] = Membership[A] with Consensus

  def broadcast[A: Tag](data: A): ZIO[Membership[A], keeper.Error, Unit] =
    ZIO.accessM(_.get.broadcast(data))

  def send[A: Tag](data: A, recepient: NodeAddress): ZIO[Membership[A], keeper.Error, Unit] =
    ZIO.accessM(_.get.send(data, recepient))

  def receive[A: Tag]: ZStream[Membership[A], Nothing, (NodeAddress, A)] =
    ZStream.accessStream(_.get.receive)
}

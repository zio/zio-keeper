package zio

import zio.stream._

package object keeper extends MembershipProtocol with ConsensusProtocol {
  type Keeper[A] = Membership[A] with Consensus

  def broadcast[A](data: A): ZIO[Keeper[A], Nothing, Unit] = ???

  def send[A](data: A, recepient: NodeAddress): ZIO[Keeper[A], Nothing, Unit] = ???

  def receive[A]: ZStream[Keeper[A], Nothing, (NodeAddress, A)] = ???
}

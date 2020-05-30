package zio.keeper

import zio._
import zio.stream._

trait MembershipProtocol {

  type Membership[A] = Has[Membership.Service[A]]

  object Membership {

    trait Service[A] {
      def broadcast(data: A): IO[SerializationError, Unit]
      def send(data: A, recepient: NodeAddress): UIO[Unit]
      def receive: Stream[Nothing, (NodeAddress, A)]
    }
  }
}

package zio

import zio.stream._

package object keeper {
  type Keeper[A] = Has[Keeper.Service[A]]

  object Keeper {

    trait Service[A] {
      def broadcast(data: A): UIO[Unit]
      def send(data: A, recepient: NodeAddress): UIO[Unit]
      def receive: Stream[Nothing, (NodeAddress, A)]

      // TODO: add consensus stuff
    }

    // TODO: provide live part
  }

  def broadcast[A](data: A): ZIO[Keeper[A], Nothing, Unit] = ???

  def send[A](data: A, recepient: NodeAddress): ZIO[Keeper[A], Nothing, Unit] = ???

  def receive[A]: ZStream[Keeper[A], Nothing, (NodeAddress, A)] = ???
}

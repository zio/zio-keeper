package zio.keeper.discovery

import zio.ZIO
import zio.keeper.Error

trait Discovery[A] {
  def discover: Discovery.Service[Any, A]
}

object Discovery {

  def staticList[A](addresses: Set[A]): Discovery[A] = new Discovery[A] {

    override def discover = new Service[Any, A] {

      override def discoverNodes: ZIO[Any, Error, Set[A]] =
        ZIO.succeed(addresses)
    }
  }

  trait Service[R, A] {
    def discoverNodes: ZIO[R, Error, Set[A]]
  }
}

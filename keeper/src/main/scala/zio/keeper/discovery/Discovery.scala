package zio.keeper.discovery

import zio.ZIO
import zio.keeper.Error
import zio.nio.core.InetSocketAddress

trait Discovery {
  def discover: Discovery.Service[Any]
}

object Discovery {

  def staticList(addresses: Set[InetSocketAddress]): Discovery = new Discovery {

    override def discover = new Service[Any] {

      override def discoverNodes: ZIO[Any, Error, Set[InetSocketAddress]] =
        ZIO.succeed(addresses)
    }
  }

  trait Service[R] {
    def discoverNodes: ZIO[R, Error, Set[InetSocketAddress]]
  }
}

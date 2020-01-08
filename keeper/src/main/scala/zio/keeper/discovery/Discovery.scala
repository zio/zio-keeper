package zio.keeper.discovery

import zio.ZIO
import zio.keeper.Error
import zio.nio.SocketAddress

trait Discovery {
  def discover: Discovery.Service[Any]
}

object Discovery {

  def staticList(addresses: Set[SocketAddress]): Discovery = new Discovery {

    override def discover: Service[Any] = new Service[Any] {

      override def discoverNodes: ZIO[Any, Error, Set[SocketAddress]] =
        ZIO.succeed(addresses)
    }
  }

  trait Service[R] {
    def discoverNodes: ZIO[R, Error, Set[zio.nio.SocketAddress]]
  }
}

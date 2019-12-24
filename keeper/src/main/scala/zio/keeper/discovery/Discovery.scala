package zio.keeper.discovery

import zio.ZIO
import zio.keeper.Error
import zio.nio.SocketAddress

trait Discovery {
  val discover: Discovery.Service[Any]
}

object Discovery {

  def staticList(addresses: Set[SocketAddress]): Discovery = new Discovery {

    override val discover: Service[Any] = new Service[Any] {

      override val discoverNodes: ZIO[Any, Error, Set[SocketAddress]] =
        ZIO.succeed(addresses)
    }
  }

  trait Service[R] {
    val discoverNodes: ZIO[R, Error, Set[zio.nio.SocketAddress]]
  }

}

package zio.keeper.discovery.static

import zio.{ Layer, UIO, ZLayer }
import zio.keeper.discovery.Discovery
import zio.nio.core.SocketAddress

object Static {

  def live(addresses: Set[SocketAddress]): Layer[Nothing, Discovery] =
    ZLayer.succeed {
      new Discovery.Service {
        final override val discoverNodes: UIO[Set[SocketAddress]] =
          UIO.succeed(addresses)
      }
    }
}

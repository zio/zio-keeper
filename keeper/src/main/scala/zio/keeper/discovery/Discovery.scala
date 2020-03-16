package zio.keeper.discovery

import zio.{ IO, Layer, UIO, ZLayer }
import zio.keeper.Error
import zio.nio.core.SocketAddress

object Discovery {

  trait Service {
    def discoverNodes: IO[Error, Set[SocketAddress]]
  }

  def staticList(addresses: Set[SocketAddress]): Layer[Nothing, Discovery] =
    ZLayer.succeed {
      new Service {
        final override val discoverNodes: UIO[Set[SocketAddress]] =
          UIO.succeed(addresses)
      }
    }
}

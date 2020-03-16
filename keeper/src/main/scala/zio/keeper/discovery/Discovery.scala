package zio.keeper.discovery

import zio.IO
import zio.keeper.Error
import zio.nio.core.SocketAddress

object Discovery {

  trait Service {
    def discoverNodes: IO[Error, Set[SocketAddress]]
  }
}

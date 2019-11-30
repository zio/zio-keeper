package zio.keeper.discovery

import zio.ZIO
import zio.console.Console
import zio.keeper.Error

trait Discovery {
  val discover: ZIO[Console, Error, Set[zio.nio.SocketAddress]]
}

package zio.membership

import zio.ZIO
import zio.nio.core.SocketAddress

package object discovery extends Discovery.Service[Discovery] {

  override def discoverNodes: ZIO[Discovery, Error, Set[SocketAddress]] =
    ZIO.accessM(_.discover.discoverNodes)
}

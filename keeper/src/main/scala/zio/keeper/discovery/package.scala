package zio.keeper

import zio.{ Has, ZIO }
import zio.nio.core.SocketAddress

package object discovery {

  type Discovery = Has[Discovery.Service]

  def discoverNodes: ZIO[Discovery, Error, Set[SocketAddress]] =
    ZIO.accessM(_.get.discoverNodes)
}

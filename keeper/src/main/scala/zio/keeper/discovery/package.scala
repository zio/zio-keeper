package zio.keeper

import zio.{ Has, ZIO }
import zio.nio.core.InetSocketAddress

package object discovery {

  type Discovery = Has[Discovery.Service]

  def discoverNodes: ZIO[Discovery, Error, Set[InetSocketAddress]] =
    ZIO.accessM(_.get.discoverNodes)
}

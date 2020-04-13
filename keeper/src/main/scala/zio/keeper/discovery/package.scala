package zio.keeper

import zio.nio.core.InetSocketAddress
import zio.{ Has, ZIO }

package object discovery {

  type Discovery = Has[Discovery.Service]

  def discoverNodes: ZIO[Discovery, Error, Set[InetSocketAddress]] =
    ZIO.accessM(_.get.discoverNodes)
}

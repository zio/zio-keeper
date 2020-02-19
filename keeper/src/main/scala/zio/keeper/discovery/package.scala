package zio.keeper
import zio.ZIO
import zio.nio.core.InetSocketAddress

package object discovery extends Discovery.Service[Discovery] {

  override def discoverNodes: ZIO[Discovery, Error, Set[InetSocketAddress]] =
    ZIO.accessM(_.discover.discoverNodes)
}

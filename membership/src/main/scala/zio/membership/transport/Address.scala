package zio.membership.transport

import zio.IO
import zio.nio.core.SocketAddress
import zio.nio.core.InetSocketAddress
import zio.membership.ResolutionFailed
import upickle.default._

final case class Address(
  host: String,
  port: Int
) { self =>

  val toInetSocketAddres: IO[ResolutionFailed, InetSocketAddress] =
    SocketAddress.inetSocketAddress(host, port).mapError(ResolutionFailed(self, _))

  override def toString() =
    s"$host:$port"

}

object Address {
  implicit val addressRW = macroRW[Address]
}

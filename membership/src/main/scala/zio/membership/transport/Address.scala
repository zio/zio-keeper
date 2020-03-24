package zio.membership.transport

import zio.IO
import zio.membership.ResolutionFailed
import zio.nio.core.{ InetSocketAddress, SocketAddress }

final case class Address(
  host: String,
  port: Int
) { self =>

  val toInetSocketAddress: IO[ResolutionFailed, InetSocketAddress] =
    SocketAddress.inetSocketAddress(host, port).mapError(ResolutionFailed(self, _))

  override def toString() =
    s"$host:$port"
}

package zio.keeper.transport

import zio.IO
import zio.nio.core.{ InetSocketAddress, SocketAddress }
import zio.keeper.TransportError.ResolutionFailed

final case class Address(
  host: String,
  port: Int
) { self =>

  val toInetSocketAddress: IO[ResolutionFailed, InetSocketAddress] =
    SocketAddress.inetSocketAddress(host, port).mapError(_ => ResolutionFailed(self))

  override def toString() =
    s"$host:$port"
}

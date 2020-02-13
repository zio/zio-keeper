package zio.keeper

import zio.nio.SocketAddress
import zio.{ ZIO, ZManaged }

package object transport extends Transport.Service[Transport[SocketAddress], SocketAddress] {

  override def bind(
    localAddr: SocketAddress
  )(
    connectionHandler: Connection[SocketAddress] => ZIO[Any, Nothing, Unit]
  ): ZManaged[Transport[SocketAddress], TransportError, Bind[SocketAddress]] =
    ZManaged.environment[Transport[SocketAddress]].flatMap(_.transport.bind(localAddr)(connectionHandler))

  override def connect(to: SocketAddress): ZManaged[Transport[SocketAddress], TransportError, Connection[SocketAddress]] =
    ZManaged.environment[Transport[SocketAddress]].flatMap(_.transport.connect(to))
}

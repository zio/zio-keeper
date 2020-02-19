package zio.keeper

import zio.nio.core.InetSocketAddress
import zio.{ ZIO, ZManaged }

package object transport extends Transport.Service[Transport] {

  override def bind(
    localAddr: InetSocketAddress
  )(
    connectionHandler: Connection => ZIO[Any, Nothing, Unit]
  ): ZManaged[Transport, TransportError, Bind] =
    ZManaged.environment[Transport].flatMap(_.transport.bind(localAddr)(connectionHandler))

  override def connect(to: InetSocketAddress): ZManaged[Transport, TransportError, Connection] =
    ZManaged.environment[Transport].flatMap(_.transport.connect(to))
}

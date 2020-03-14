package zio.keeper

import zio.nio.core.SocketAddress
import zio.{ Has, ZIO, ZManaged }
import zio.keeper.transport.Channel._

package object transport {

  type Transport = Has[Transport.Service]

  def bind(
    localAddr: SocketAddress
  )(connectionHandler: Connection => ZIO[Any, Nothing, Unit]): ZManaged[Transport, TransportError, Bind] =
    ZManaged.environment[Transport].flatMap(_.get.bind(localAddr)(connectionHandler))

  def connect(to: SocketAddress): ZManaged[Transport, TransportError, Connection] =
    ZManaged.environment[Transport].flatMap(_.get.connect(to))
}

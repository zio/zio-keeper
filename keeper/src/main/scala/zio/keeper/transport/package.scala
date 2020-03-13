package zio.keeper

import zio.nio.core.SocketAddress
import zio.{ Has, ZIO, ZManaged }

package object transport {

  type Transport = Has[Transport.Service]

  def bind(
    localAddr: SocketAddress
  )(connectionHandler: ChannelOut => ZIO[Any, Nothing, Unit]): ZManaged[Transport, TransportError, ChannelIn] =
    ZManaged.environment[Transport].flatMap(_.get.bind(localAddr)(connectionHandler))

  def connect(to: SocketAddress): ZManaged[Transport, TransportError, ChannelOut] =
    ZManaged.environment[Transport].flatMap(_.get.connect(to))
}

package zio.keeper

import zio.nio.SocketAddress
import zio.{ ZIO, ZManaged }

package object transport extends Transport.Service[Transport] {

  override def bind(
    localAddr: SocketAddress
  )(connectionHandler: ChannelOut => ZIO[Any, Nothing, Unit]): ZManaged[Transport, TransportError, ChannelIn] =
    ZManaged.environment[Transport].flatMap(_.transport.bind(localAddr)(connectionHandler))

  override def connect(to: SocketAddress): ZManaged[Transport, TransportError, ChannelOut] =
    ZManaged.environment[Transport].flatMap(_.transport.connect(to))
}

package zio.keeper.transport

import zio.keeper.TransportError
import zio.nio.core.SocketAddress
import zio.{ Managed, UIO }

object Transport {

  trait Service {
    def bind(localAddr: SocketAddress)(connectionHandler: ChannelOut => UIO[Unit]): Managed[TransportError, ChannelIn]
    def connect(to: SocketAddress): Managed[TransportError, ChannelOut]
  }
}

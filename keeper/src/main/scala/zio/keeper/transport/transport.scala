package zio.keeper.transport

import zio.keeper.TransportError
import zio.nio.core.SocketAddress
import zio.{ Managed, UIO }
import zio.keeper.transport.Channel._

object Transport {

  trait Service {
    def bind(localAddr: SocketAddress)(connectionHandler: Connection => UIO[Unit]): Managed[TransportError, Bind]
    def connect(to: SocketAddress): Managed[TransportError, Connection]
  }
}

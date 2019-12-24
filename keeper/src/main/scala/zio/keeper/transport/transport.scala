package zio.keeper.transport

import zio.keeper.TransportError
import zio.nio.SocketAddress
import zio.{Chunk, UIO, ZIO, ZManaged}

trait Transport {
  val transport: Transport.Service[Any]
}

object Transport {

  trait Service[R] {
    def bind(localAddr: SocketAddress)(
      connectionHandler: ChannelOut => UIO[Unit]
    ): ZManaged[R, TransportError, ChannelIn]

    def connect(to: SocketAddress): ZManaged[R, TransportError, ChannelOut]
  }

}

sealed trait Channel {


  val isOpen: ZIO[Any, TransportError, Boolean]

  val close: ZIO[Any, TransportError, Unit]
}

trait ChannelOut extends Channel {

  val read: ZIO[Any, TransportError, Chunk[Byte]]

  def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit]
}

trait ChannelIn extends Channel {}

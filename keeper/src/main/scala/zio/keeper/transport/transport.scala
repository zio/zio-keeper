package zio.keeper.transport

import zio.keeper.TransportError
import zio.nio.SocketAddress
import zio.{Chunk, ZIO, ZManaged}

trait Transport {
  val transport: Transport.Service[Any]
}

object Transport {

  trait Service[R] {
    def connect(to: SocketAddress): ZManaged[R, TransportError, ChannelOut]
    def bind(localAddr: SocketAddress)(connectionHandler: ChannelOut => ZIO[Any, Nothing, Unit]): ZManaged[R, TransportError, ChannelIn]
  }
}

sealed trait Channel {
  def isOpen: ZIO[Any, TransportError, Boolean]
  def close: ZIO[Any, TransportError, Unit]
}

trait ChannelOut extends Channel {
  def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit]
  def read: ZIO[Any, TransportError, Chunk[Byte]]
}

trait ChannelIn extends Channel {
}



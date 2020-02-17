package zio.keeper.transport

import zio.keeper.TransportError
import zio.nio.core.InetSocketAddress
import zio.{Chunk, UIO, ZIO, ZManaged}

trait Transport {
  val transport: Transport.Service[Any]
}

object Transport {

  trait Service[R] {

    def bind(localAddr: InetSocketAddress)(
      connectionHandler: Connection => UIO[Unit]
    ): ZManaged[R, TransportError, Bind]

    def connect(to: InetSocketAddress): ZManaged[R, TransportError, Connection]
  }

}

sealed trait Channel {
  def address: InetSocketAddress

  def close: ZIO[Any, TransportError, Unit]

  def isOpen: ZIO[Any, TransportError, Boolean]
}

trait Connection extends Channel {
  def read: ZIO[Any, TransportError, Chunk[Byte]]

  def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit]
}

trait Bind extends Channel {}

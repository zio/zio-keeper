package zio.keeper.transport

import zio.keeper.TransportError
import zio.{Chunk, UIO, ZIO, ZManaged}

trait Transport[A] {
  val transport: Transport.Service[Any, A]
}

object Transport {

  trait Service[R, A] {

    def bind(localAddr: A)(
      connectionHandler: Connection => UIO[Unit]
    ): ZManaged[R, TransportError, Bind]

    def connect(to: A): ZManaged[R, TransportError, Connection]
  }

}

sealed trait Channel {
  def close: ZIO[Any, TransportError, Unit]

  def isOpen: ZIO[Any, TransportError, Boolean]
}

trait Connection extends Channel {
  def read: ZIO[Any, TransportError, Chunk[Byte]]

  def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit]
}

trait Bind extends Channel {}

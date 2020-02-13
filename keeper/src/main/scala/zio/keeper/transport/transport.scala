package zio.keeper.transport

import zio.keeper.TransportError
import zio.{Chunk, UIO, ZIO, ZManaged}

trait Transport[A] {
  val transport: Transport.Service[Any, A]
}

object Transport {

  trait Service[R, A] {

    def bind(localAddr: A)(
      connectionHandler: Connection[A] => UIO[Unit]
    ): ZManaged[R, TransportError, Bind[A]]

    def connect(to: A): ZManaged[R, TransportError, Connection[A]]
  }

}

sealed trait Channel[A] {
  def address: A

  def close: ZIO[Any, TransportError, Unit]

  def isOpen: ZIO[Any, TransportError, Boolean]
}

trait Connection[A] extends Channel[A] {
  def read: ZIO[Any, TransportError, Chunk[Byte]]

  def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit]
}

trait Bind[A] extends Channel[A] {}

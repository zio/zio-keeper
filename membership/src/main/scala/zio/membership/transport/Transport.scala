package zio.membership.transport

import zio._
import zio.stream._
import zio.membership.TransportError

trait Transport[T] {
  val transport: Transport.Service[Any, T]
}

object Transport {

  trait Service[R, T] {
    def send(to: T, data: Chunk[Byte]): ZIO[R, TransportError, Unit] =
      connect(to).use(_.send(data))
    def connect(to: T): ZManaged[R, TransportError, Connection[R, TransportError, Chunk[Byte]]]
    def bind(addr: T): ZStream[R, TransportError, Connection[R, TransportError, Chunk[Byte]]]
  }
}

package zio.membership.transport

import zio._
import zio.stream._
import zio.membership.TransportError

trait Transport[T] {
  val transport: Transport.Service[Any, T]
}

object Transport {

  def send[T](to: T, data: Chunk[Byte]): ZIO[Transport[T], TransportError, Unit] =
    ZIO.accessM[Transport[T]](_.transport.send(to, data))

  def connect[T](to: T): ZManaged[Transport[T], TransportError, ChunkConnection] =
    ZManaged.environment[Transport[T]].flatMap(_.transport.connect(to))

  def bind[T](addr: T): ZStream[Transport[T], TransportError, Managed[Nothing, ChunkConnection]] =
    ZStream.unwrap(ZIO.access[Transport[T]](_.transport.bind(addr)))

  trait Service[R, T] {

    def send(to: T, data: Chunk[Byte]): ZIO[R, TransportError, Unit] =
      connect(to).use(_.send(data))

    def connect(to: T): ZManaged[R, TransportError, ChunkConnection]

    def bind(addr: T): ZStream[R, TransportError, Managed[Nothing, ChunkConnection]]
  }
}

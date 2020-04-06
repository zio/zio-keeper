package zio.membership.transport

import zio._
import zio.stream._

import zio.membership.TransportError

object Transport {

  def bind[T: Tagged](addr: T): ZStream[Transport[T], TransportError, ChunkConnection] =
    ZStream.unwrap(ZIO.access[Transport[T]](_.get.bind(addr)))

  def connect[T: Tagged](to: T): ZManaged[Transport[T], TransportError, ChunkConnection] =
    ZManaged.environment[Transport[T]].flatMap(_.get.connect(to))

  def send[T: Tagged](to: T, data: Chunk[Byte]): ZIO[Transport[T], TransportError, Unit] =
    ZIO.accessM[Transport[T]](_.get.send(to, data))

  trait Service[T] {
    def bind(addr: T): Stream[TransportError, ChunkConnection]
    def connect(to: T): Managed[TransportError, ChunkConnection]

    def send(to: T, data: Chunk[Byte]): IO[TransportError, Unit] =
      connect(to).use(_.send(data))
  }
}

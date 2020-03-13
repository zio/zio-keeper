package zio.membership.transport

import zio.{ Chunk, IO, Managed }
import zio.membership.TransportError
import zio.stream.Stream

object Transport {

  trait Service[T] {
    def bind(addr: T): Stream[TransportError, Managed[Nothing, ChunkConnection]]
    def connect(to: T): Managed[TransportError, ChunkConnection]

    def send(to: T, data: Chunk[Byte]): IO[TransportError, Unit] =
      connect(to).use(_.send(data))
  }
}

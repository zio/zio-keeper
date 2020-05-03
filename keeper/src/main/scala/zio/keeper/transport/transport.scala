package zio.keeper.transport

import zio._
import zio.stream._
import zio.keeper.{ NodeAddress, TransportError }

object Transport {

  def bind(addr: NodeAddress): ZStream[Transport, TransportError, ChunkConnection] =
    ZStream.accessStream(_.get.bind(addr))

  def connect(to: NodeAddress): ZManaged[Transport, TransportError, ChunkConnection] =
    ZManaged.accessManaged(_.get.connect(to))

  def send(to: NodeAddress, data: Chunk[Byte]): ZIO[Transport, TransportError, Unit] =
    ZIO.accessM(_.get.send(to, data))

  trait Service {
    def bind(addr: NodeAddress): Stream[TransportError, ChunkConnection]
    def connect(to: NodeAddress): Managed[TransportError, ChunkConnection]

    def send(to: NodeAddress, data: Chunk[Byte]): IO[TransportError, Unit] =
      connect(to).use(_.send(data))
  }
}

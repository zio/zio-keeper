package zio.membership

import zio.{ Chunk, Has, Managed, Tagged, ZIO, ZManaged }
import zio.stream.ZStream

package object transport {

  type Transport[T] = Has[Transport.Service[T]]

  type ChunkConnection = Connection[Any, TransportError, Chunk[Byte]]

  def send[T: Tagged](to: T, data: Chunk[Byte]): ZIO[Transport[T], TransportError, Unit] =
    ZIO.accessM[Transport[T]](_.get.send(to, data))

  def connect[T: Tagged](to: T): ZManaged[Transport[T], TransportError, ChunkConnection] =
    ZManaged.environment[Transport[T]].flatMap(_.get.connect(to))

  def bind[T: Tagged](addr: T): ZStream[Transport[T], TransportError, Managed[Nothing, ChunkConnection]] =
    ZStream.unwrap(ZIO.access[Transport[T]](_.get.bind(addr)))
}

package zio.membership

import zio.{ Chunk, Has }

package object transport {

  type Transport[T] = Has[Transport.Service[T]]

  type ChunkConnection = Connection[Any, TransportError, Chunk[Byte]]

}

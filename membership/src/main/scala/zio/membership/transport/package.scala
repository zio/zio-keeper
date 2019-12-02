package zio.membership

import zio._

package object transport {

  type ChunkConnection = Connection[Any, TransportError, Chunk[Byte]]

}
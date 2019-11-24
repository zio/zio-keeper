package zio.membership

import zio._

trait ByteCodec[-R, A] {
  def toChunk(a: A): URIO[R, Chunk[Byte]]
  def fromChunk(chunk: Chunk[Byte]): ZIO[R, DeserializationError, (Chunk[Byte], A)]
}

object MessageCodec {

  def apply[R, A](implicit ev: ByteCodec[R, A]): ByteCodec[R, A] =
    ev

}

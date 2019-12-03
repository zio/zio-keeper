package zio.membership

import zio._
import upickle.default._

trait ByteCodec[A] {
  def fromChunk(chunk: Chunk[Byte]): IO[DeserializationError, A]
  def toChunk(a: A): IO[SerializationError, Chunk[Byte]]
}

object ByteCodec {

  def apply[A](implicit ev: ByteCodec[A]): ByteCodec[A] =
    ev

  def fromReadWriter[A](rw: ReadWriter[A]): ByteCodec[A] =
    new ByteCodec[A] {

      def toChunk(a: A) =
        ZIO.effect(Chunk.fromArray(writeBinary(a)(rw))).mapError(SerializationError("Failed serializing to chunk.", _))

      def fromChunk(chunk: Chunk[Byte]) =
        ZIO.effect(readBinary[A](chunk.toArray)(rw)).mapError(DeserializationError("Failed deserializing chunk.", _))
    }
}

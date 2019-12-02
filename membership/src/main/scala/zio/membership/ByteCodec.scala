package zio.membership

import zio._
import upickle.default._

trait ByteCodec[A] {
  def fromChunk(chunk: Chunk[Byte]): IO[DeserializationError, A]
  def toChunk(a: A): IO[SerializationError, Chunk[Byte]]
}

object ByteCodec {

  def apply[R, A](implicit ev: ByteCodec[A]): ByteCodec[A] =
    ev

  def fromReadWriter[A](implicit ev: ReadWriter[A]): ByteCodec[A] =
    new ByteCodec[A] {

      def toChunk(a: A) =
        ZIO.effect(Chunk.fromArray(writeBinary(a))).mapError(SerializationError("Failed serializing to chunk.", _))

      def fromChunk(chunk: Chunk[Byte]) =
        ZIO.effect(readBinary[A](chunk.toArray)).mapError(DeserializationError("Failed deserializing chunk.", _))
    }

  def encode[A](data: A)(implicit ev: ByteCodec[A]): IO[SerializationError, Chunk[Byte]] =
    ev.toChunk(data)

  def decode[A](chunk: Chunk[Byte])(implicit ev: ByteCodec[A]): IO[DeserializationError, A] =
    ev.fromChunk(chunk)

}

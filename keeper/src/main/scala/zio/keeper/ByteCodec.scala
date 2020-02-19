package zio.keeper

import zio._
import upickle.default._
import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }

trait ByteCodec[A] {
  def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A]
  def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]]
}

object ByteCodec {

  def apply[A](implicit ev: ByteCodec[A]): ByteCodec[A] =
    ev

  def fromReadWriter[A](rw: ReadWriter[A]): ByteCodec[A] =
    new ByteCodec[A] {

      def toChunk(a: A) =
        ZIO.effect(Chunk.fromArray(writeBinary(a)(rw))).mapError(SerializationTypeError(_))

      def fromChunk(chunk: Chunk[Byte]) =
        ZIO.effect(readBinary[A](chunk.toArray)(rw)).mapError(DeserializationTypeError(_))
    }
}

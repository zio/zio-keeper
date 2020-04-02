package zio.keeper.membership

import zio._
import upickle.default._
import zio.keeper.SerializationError.DeserializationTypeError
import zio.keeper.SerializationError.SerializationTypeError

trait ByteCodec[A] {
  def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A]
  def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]]
}

object ByteCodec {

  def apply[A](implicit ev: ByteCodec[A]): ByteCodec[A] =
    ev

  def instance[A](
    f: Chunk[Byte] => IO[DeserializationTypeError, A]
  )(g: A => IO[SerializationTypeError, Chunk[Byte]]): ByteCodec[A] =
    new ByteCodec[A] {
      override def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A] = f(chunk)

      override def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]] = g(a)
    }

  def fromChunk[A: ByteCodec](chunk: Chunk[Byte]): IO[DeserializationTypeError, A] =
    ByteCodec[A].fromChunk(chunk)

  def toChunk[A: ByteCodec](a: A): IO[SerializationTypeError, Chunk[Byte]] =
    ByteCodec[A].toChunk(a)

  def fromReadWriter[A](rw: ReadWriter[A]): ByteCodec[A] =
    new ByteCodec[A] {

      def toChunk(a: A) =
        ZIO.effect(Chunk.fromArray(writeBinary(a)(rw))).mapError(SerializationTypeError(_))

      def fromChunk(chunk: Chunk[Byte]) =
        ZIO.effect(readBinary[A](chunk.toArray)(rw)).mapError(DeserializationTypeError(_))
    }
}

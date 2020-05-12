package zio.keeper

import upickle.default._
import zio._
import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import scala.reflect.ClassTag

trait ByteCodec[A] { self =>
  def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A]

  def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]]

  def bimap[B, C](f: A => B, g: B => A): ByteCodec[B] =
    ByteCodec.instance(self.fromChunk(_).map(f))((self.toChunk _).compose(g))

  private[ByteCodec] def unsafeWiden[A1 >: A](implicit tag: ClassTag[A]): ByteCodec[A1] =
    new ByteCodec[A1] {

      def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A1] =
        self.fromChunk(chunk)

      def toChunk(a1: A1): IO[SerializationTypeError, Chunk[Byte]] =
        a1 match {
          case a: A => self.toChunk(a)
          case _    => IO.fail(SerializationTypeError(s"Unsupported type ${a1.getClass}"))
        }
    }
}

object ByteCodec {

  final class TaggedBuilder[A] {

    def apply[A1 <: A: ByteCodec: ClassTag] =
      taggedInstance[A](
        {
          case _: A1 => 0
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
        }
      )

    def apply[A1 <: A: ByteCodec: ClassTag, A2 <: A: ByteCodec: ClassTag] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
        }
      )

    def apply[A1 <: A: ByteCodec: ClassTag, A2 <: A: ByteCodec: ClassTag, A3 <: A: ByteCodec: ClassTag] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: ByteCodec: ClassTag,
      A2 <: A: ByteCodec: ClassTag,
      A3 <: A: ByteCodec: ClassTag,
      A4 <: A: ByteCodec: ClassTag
    ] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
          case 3 => ByteCodec[A4].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: ByteCodec: ClassTag,
      A2 <: A: ByteCodec: ClassTag,
      A3 <: A: ByteCodec: ClassTag,
      A4 <: A: ByteCodec: ClassTag,
      A5 <: A: ByteCodec: ClassTag
    ] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
          case 3 => ByteCodec[A4].unsafeWiden[A]
          case 4 => ByteCodec[A5].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: ByteCodec: ClassTag,
      A2 <: A: ByteCodec: ClassTag,
      A3 <: A: ByteCodec: ClassTag,
      A4 <: A: ByteCodec: ClassTag,
      A5 <: A: ByteCodec: ClassTag,
      A6 <: A: ByteCodec: ClassTag
    ] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
          case _: A6 => 5
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
          case 3 => ByteCodec[A4].unsafeWiden[A]
          case 4 => ByteCodec[A5].unsafeWiden[A]
          case 5 => ByteCodec[A6].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: ByteCodec: ClassTag,
      A2 <: A: ByteCodec: ClassTag,
      A3 <: A: ByteCodec: ClassTag,
      A4 <: A: ByteCodec: ClassTag,
      A5 <: A: ByteCodec: ClassTag,
      A6 <: A: ByteCodec: ClassTag,
      A7 <: A: ByteCodec: ClassTag
    ] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
          case _: A6 => 5
          case _: A7 => 6
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
          case 3 => ByteCodec[A4].unsafeWiden[A]
          case 4 => ByteCodec[A5].unsafeWiden[A]
          case 5 => ByteCodec[A6].unsafeWiden[A]
          case 6 => ByteCodec[A7].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: ByteCodec: ClassTag,
      A2 <: A: ByteCodec: ClassTag,
      A3 <: A: ByteCodec: ClassTag,
      A4 <: A: ByteCodec: ClassTag,
      A5 <: A: ByteCodec: ClassTag,
      A6 <: A: ByteCodec: ClassTag,
      A7 <: A: ByteCodec: ClassTag,
      A8 <: A: ByteCodec: ClassTag
    ] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
          case _: A6 => 5
          case _: A7 => 6
          case _: A8 => 7
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
          case 3 => ByteCodec[A4].unsafeWiden[A]
          case 4 => ByteCodec[A5].unsafeWiden[A]
          case 5 => ByteCodec[A6].unsafeWiden[A]
          case 6 => ByteCodec[A7].unsafeWiden[A]
          case 7 => ByteCodec[A8].unsafeWiden[A]
        }
      )
  }

  def apply[A](implicit ev: ByteCodec[A]): ByteCodec[A] =
    ev

  def instance[A](
    f: Chunk[Byte] => IO[DeserializationTypeError, A]
  )(g: A => IO[SerializationTypeError, Chunk[Byte]]): ByteCodec[A] =
    new ByteCodec[A] {
      override def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A] = f(chunk)

      override def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]] = g(a)
    }

  def tagged[A]: TaggedBuilder[A] =
    new TaggedBuilder[A]()

  def taggedInstance[A](f: PartialFunction[A, Byte], g: PartialFunction[Byte, ByteCodec[A]]): ByteCodec[A] = {

    def tagOf(a: A) =
      if (f.isDefinedAt(a)) ZIO.succeed(f(a))
      else ZIO.fail(())

    def codecFor(tag: Byte) =
      if (g.isDefinedAt(tag)) ZIO.succeed(g(tag))
      else ZIO.fail(())

    new ByteCodec[A] {

      def fromChunk(from: Chunk[Byte]): IO[DeserializationTypeError, A] =
        if (from.isEmpty) ZIO.fail(DeserializationTypeError("Empty chunk"))
        else {
          val (tag, chunk) = from.splitAt(1)
          for {
            codec <- codecFor(tag(0)).mapError(_ => DeserializationTypeError(s"No codec found for tag ${tag(0)}"))
            data  <- codec.fromChunk(chunk)
          } yield data
        }

      def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]] =
        for {
          tag   <- tagOf(a).mapError(_ => SerializationTypeError(s"No tag found for type ${a.getClass}"))
          codec <- codecFor(tag).mapError(_ => SerializationTypeError(s"No codec found for tag $tag"))
          chunk <- codec.toChunk(a).map(Chunk.single(tag) ++ _)
        } yield chunk
    }
  }

  def decode[A: ByteCodec](chunk: Chunk[Byte]): IO[DeserializationTypeError, A] =
    ByteCodec[A].fromChunk(chunk)

  def encode[A: ByteCodec](a: A): IO[SerializationTypeError, Chunk[Byte]] =
    ByteCodec[A].toChunk(a)

  def fromReadWriter[A](rw: ReadWriter[A]): ByteCodec[A] =
    new ByteCodec[A] {

      def toChunk(a: A) =
        ZIO.effect(Chunk.fromArray(writeBinary(a)(rw))).mapError(SerializationTypeError(_))

      def fromChunk(chunk: Chunk[Byte]) =
        ZIO.effect(readBinary[A](chunk.toArray)(rw)).mapError(DeserializationTypeError(_))
    }
}

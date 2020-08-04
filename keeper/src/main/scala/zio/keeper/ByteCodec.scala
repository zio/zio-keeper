package zio.keeper

import java.nio.charset.StandardCharsets
import java.util.UUID

import zio._
import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import zio.keeper.encoding._

import upickle.default._

import scala.reflect.ClassTag

trait ByteCodec[A] { self =>
  def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A]

  def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]]

  def zip[B](that: ByteCodec[B]): ByteCodec[(A, B)] =
    ByteCodec.instance { chunk =>
      val (sizeChunk, dataChunk) = chunk.splitAt(4)
      for {
        split  <- byteArrayToInt(sizeChunk.toArray)
        first  <- self.fromChunk(dataChunk.take(split))
        second <- that.fromChunk(dataChunk.drop(split))
      } yield (first, second)
    } {
      case (first, second) =>
        self.toChunk(first).zipWith(that.toChunk(second)) {
          case (firstChunk, secondChunk) =>
            val sizeChunk = Chunk.fromArray(intToByteArray(firstChunk.size))
            sizeChunk ++ firstChunk ++ secondChunk
        }
    }

  def bimap[B](f: A => B, g: B => A): ByteCodec[B] =
    ByteCodec.instance(self.fromChunk(_).map(f))((self.toChunk _).compose(g))

  def bimapM[B](f: A => IO[DeserializationTypeError, B], g: B => IO[SerializationTypeError, A]): ByteCodec[B] =
    ByteCodec.instance(self.fromChunk(_).flatMap(f))(g(_).flatMap(self.toChunk))

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

    def apply[A1 <: A: ByteCodec: ClassTag]: ByteCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
        }
      )

    def apply[A1 <: A: ByteCodec: ClassTag, A2 <: A: ByteCodec: ClassTag]: ByteCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: ByteCodec: ClassTag,
      A2 <: A: ByteCodec: ClassTag,
      A3 <: A: ByteCodec: ClassTag
    ]: ByteCodec[A] =
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
    ]: ByteCodec[A] =
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
    ]: ByteCodec[A] =
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
    ]: ByteCodec[A] =
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
    ]: ByteCodec[A] =
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
    ]: ByteCodec[A] =
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

    def apply[
      A1 <: A: ByteCodec: ClassTag,
      A2 <: A: ByteCodec: ClassTag,
      A3 <: A: ByteCodec: ClassTag,
      A4 <: A: ByteCodec: ClassTag,
      A5 <: A: ByteCodec: ClassTag,
      A6 <: A: ByteCodec: ClassTag,
      A7 <: A: ByteCodec: ClassTag,
      A8 <: A: ByteCodec: ClassTag,
      A9 <: A: ByteCodec: ClassTag
    ]: ByteCodec[A] =
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
          case _: A9 => 8
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
          case 3 => ByteCodec[A4].unsafeWiden[A]
          case 4 => ByteCodec[A5].unsafeWiden[A]
          case 5 => ByteCodec[A6].unsafeWiden[A]
          case 6 => ByteCodec[A7].unsafeWiden[A]
          case 7 => ByteCodec[A8].unsafeWiden[A]
          case 8 => ByteCodec[A9].unsafeWiden[A]
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
      A8 <: A: ByteCodec: ClassTag,
      A9 <: A: ByteCodec: ClassTag,
      A10 <: A: ByteCodec: ClassTag
    ]: ByteCodec[A] =
      taggedInstance[A](
        {
          case _: A1  => 0
          case _: A2  => 1
          case _: A3  => 2
          case _: A4  => 3
          case _: A5  => 4
          case _: A6  => 5
          case _: A7  => 6
          case _: A8  => 7
          case _: A9  => 8
          case _: A10 => 9
        }, {
          case 0 => ByteCodec[A1].unsafeWiden[A]
          case 1 => ByteCodec[A2].unsafeWiden[A]
          case 2 => ByteCodec[A3].unsafeWiden[A]
          case 3 => ByteCodec[A4].unsafeWiden[A]
          case 4 => ByteCodec[A5].unsafeWiden[A]
          case 5 => ByteCodec[A6].unsafeWiden[A]
          case 6 => ByteCodec[A7].unsafeWiden[A]
          case 7 => ByteCodec[A8].unsafeWiden[A]
          case 8 => ByteCodec[A9].unsafeWiden[A]
          case 9 => ByteCodec[A10].unsafeWiden[A]
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
      A8 <: A: ByteCodec: ClassTag,
      A9 <: A: ByteCodec: ClassTag,
      A10 <: A: ByteCodec: ClassTag,
      A11 <: A: ByteCodec: ClassTag
    ]: ByteCodec[A] =
      taggedInstance[A](
        {
          case _: A1  => 0
          case _: A2  => 1
          case _: A3  => 2
          case _: A4  => 3
          case _: A5  => 4
          case _: A6  => 5
          case _: A7  => 6
          case _: A8  => 7
          case _: A9  => 8
          case _: A10 => 9
          case _: A11 => 10
        }, {
          case 0  => ByteCodec[A1].unsafeWiden[A]
          case 1  => ByteCodec[A2].unsafeWiden[A]
          case 2  => ByteCodec[A3].unsafeWiden[A]
          case 3  => ByteCodec[A4].unsafeWiden[A]
          case 4  => ByteCodec[A5].unsafeWiden[A]
          case 5  => ByteCodec[A6].unsafeWiden[A]
          case 6  => ByteCodec[A7].unsafeWiden[A]
          case 7  => ByteCodec[A8].unsafeWiden[A]
          case 8  => ByteCodec[A9].unsafeWiden[A]
          case 9  => ByteCodec[A10].unsafeWiden[A]
          case 10 => ByteCodec[A10].unsafeWiden[A]
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
      A8 <: A: ByteCodec: ClassTag,
      A9 <: A: ByteCodec: ClassTag,
      A10 <: A: ByteCodec: ClassTag,
      A11 <: A: ByteCodec: ClassTag,
      A12 <: A: ByteCodec: ClassTag
    ]: ByteCodec[A] =
      taggedInstance[A](
        {
          case _: A1  => 0
          case _: A2  => 1
          case _: A3  => 2
          case _: A4  => 3
          case _: A5  => 4
          case _: A6  => 5
          case _: A7  => 6
          case _: A8  => 7
          case _: A9  => 8
          case _: A10 => 9
          case _: A11 => 10
          case _: A12 => 11
        }, {
          case 0  => ByteCodec[A1].unsafeWiden[A]
          case 1  => ByteCodec[A2].unsafeWiden[A]
          case 2  => ByteCodec[A3].unsafeWiden[A]
          case 3  => ByteCodec[A4].unsafeWiden[A]
          case 4  => ByteCodec[A5].unsafeWiden[A]
          case 5  => ByteCodec[A6].unsafeWiden[A]
          case 6  => ByteCodec[A7].unsafeWiden[A]
          case 7  => ByteCodec[A8].unsafeWiden[A]
          case 8  => ByteCodec[A9].unsafeWiden[A]
          case 9  => ByteCodec[A10].unsafeWiden[A]
          case 10 => ByteCodec[A10].unsafeWiden[A]
          case 11 => ByteCodec[A11].unsafeWiden[A]
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
      A8 <: A: ByteCodec: ClassTag,
      A9 <: A: ByteCodec: ClassTag,
      A10 <: A: ByteCodec: ClassTag,
      A11 <: A: ByteCodec: ClassTag,
      A12 <: A: ByteCodec: ClassTag,
      A13 <: A: ByteCodec: ClassTag
    ]: ByteCodec[A] =
      taggedInstance[A](
        {
          case _: A1  => 0
          case _: A2  => 1
          case _: A3  => 2
          case _: A4  => 3
          case _: A5  => 4
          case _: A6  => 5
          case _: A7  => 6
          case _: A8  => 7
          case _: A9  => 8
          case _: A10 => 9
          case _: A11 => 10
          case _: A12 => 11
          case _: A13 => 12
        }, {
          case 0  => ByteCodec[A1].unsafeWiden[A]
          case 1  => ByteCodec[A2].unsafeWiden[A]
          case 2  => ByteCodec[A3].unsafeWiden[A]
          case 3  => ByteCodec[A4].unsafeWiden[A]
          case 4  => ByteCodec[A5].unsafeWiden[A]
          case 5  => ByteCodec[A6].unsafeWiden[A]
          case 6  => ByteCodec[A7].unsafeWiden[A]
          case 7  => ByteCodec[A8].unsafeWiden[A]
          case 8  => ByteCodec[A9].unsafeWiden[A]
          case 9  => ByteCodec[A10].unsafeWiden[A]
          case 10 => ByteCodec[A10].unsafeWiden[A]
          case 11 => ByteCodec[A11].unsafeWiden[A]
          case 12 => ByteCodec[A12].unsafeWiden[A]
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

    def tagOf(a: A): IO[Unit, Byte] =
      if (f.isDefinedAt(a)) ZIO.succeed(f(a))
      else ZIO.fail(())

    def codecFor(tag: Byte): IO[Unit, ByteCodec[A]] =
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

      def toChunk(a: A): IO[SerializationTypeError, Chunk[Byte]] =
        ZIO.effect(Chunk.fromArray(writeBinary(a)(rw))).mapError(SerializationTypeError(_))

      def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, A] =
        ZIO.effect(readBinary[A](chunk.toArray)(rw)).mapError(DeserializationTypeError(_))
    }

  implicit val byteCodec: ByteCodec[Byte] =
    instance { chunk =>
      val size = chunk.length
      if (size == 1) ZIO.succeed(chunk.headOption.get)
      else ZIO.fail(DeserializationTypeError(s"Expected chunk of length 1; got $size"))
    } { elem =>
      ZIO.succeed(Chunk.single(elem))
    }

  implicit val intCodec: ByteCodec[Int] =
    instance { chunk =>
      byteArrayToInt(chunk.toArray)
    } { value =>
      ZIO.succeed(Chunk.fromArray(intToByteArray(value)))
    }

  implicit val stringCodec: ByteCodec[String] =
    instance { chunk =>
      ZIO
        .effect {
          new String(chunk.toArray, StandardCharsets.UTF_8)
        }
        .mapError(DeserializationTypeError(_))
    } { value =>
      ZIO.succeed(Chunk.fromArray(value.getBytes(StandardCharsets.UTF_8)))
    }

  implicit val uuidCodec: ByteCodec[UUID] =
    stringCodec.bimapM(
      str => ZIO.effect(UUID.fromString(str)).mapError(DeserializationTypeError(_)),
      uuid => ZIO.succeed(uuid.toString)
    )

  implicit def tupleCodec[A: ByteCodec, B: ByteCodec]: ByteCodec[(A, B)] =
    ByteCodec[A].zip(ByteCodec[B])

  implicit def chunkCodec[A: ByteCodec]: ByteCodec[Chunk[A]] =
    instance { chunk =>
      def go(remaining: Chunk[Byte], acc: List[A]): IO[DeserializationTypeError, Chunk[A]] =
        if (remaining.isEmpty) ZIO.succeed(Chunk.fromIterable(acc))
        else {
          val (sizeChunk, dataChunk) = remaining.splitAt(4)
          byteArrayToInt(sizeChunk.toArray).flatMap { elementSize =>
            ByteCodec[A].fromChunk(dataChunk.take(elementSize)).flatMap { nextA =>
              go(dataChunk.drop(elementSize), nextA :: acc)
            }
          }
        }
      go(chunk, Nil)
    } { data =>
      data.foldM(Chunk.empty: Chunk[Byte]) {
        case (acc, next) =>
          ByteCodec[A].toChunk(next).map { chunk =>
            val sizeChunk = Chunk.fromArray(intToByteArray(chunk.size))
            sizeChunk ++ chunk ++ acc
          }
      }
    }
}

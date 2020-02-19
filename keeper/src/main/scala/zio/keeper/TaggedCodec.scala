package zio.keeper

import zio._
import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }

trait TaggedCodec[A] {
  def tagOf(a: A): Byte
  def codecFor(tag: Byte): IO[Unit, ByteCodec[A]]
}

object TaggedCodec {

  def apply[A](implicit ev: TaggedCodec[A]) = ev

  def instance[A](f: A => Byte, g: PartialFunction[Byte, ByteCodec[A]]): TaggedCodec[A] =
    new TaggedCodec[A] {
      def tagOf(a: A) = f(a)

      def codecFor(tag: Byte) =
        if (g.isDefinedAt(tag)) ZIO.succeed(g(tag))
        else ZIO.fail(())
    }

  def read[A](
    from: Chunk[Byte]
  )(
    implicit
    tagged: TaggedCodec[A]
  ): ZIO[Any, DeserializationTypeError, A] =
    if (from.isEmpty) {
      ZIO.fail(DeserializationTypeError("Empty chunk"))
    } else {
      val (tag, chunk) = from.splitAt(1)
      for {
        codec <- tagged.codecFor(tag(0)).mapError(_ => DeserializationTypeError(s"No codec found for tag ${tag(0)}"))
        data  <- codec.fromChunk(chunk)
      } yield data
    }

  def write[A](
    data: A
  )(
    implicit
    tagged: TaggedCodec[A]
  ): ZIO[Any, SerializationTypeError, Chunk[Byte]] = {
    val tag = tagged.tagOf(data)
    for {
      codec <- tagged.codecFor(tag).mapError(_ => SerializationTypeError(s"No codec found for tag $tag"))
      chunk <- codec.toChunk(data).map(Chunk.single(tag) ++ _)
    } yield chunk
  }

}

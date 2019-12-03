package zio.membership.hyparview

import zio._
import zio.membership.ByteCodec
import zio.membership.DeserializationError

trait Tagged[A] {
  def tagOf(a: A): Byte
  def codecFor(tag: Byte): IO[DeserializationError, ByteCodec[A]]
}

object Tagged {

  def apply[A](implicit ev: Tagged[A]) = ev

  def instance[A](f: A => Byte, g: PartialFunction[Byte, ByteCodec[A]]): Tagged[A] =
    new Tagged[A]  {
      def tagOf(a: A) = f(a)
      def codecFor(tag: Byte) =
        if (g.isDefinedAt(tag)) ZIO.succeed(g(tag))
        else ZIO.fail(DeserializationError(s"Unknown tag '$tag'."))
    }

  private[hyparview] def read[A](
    from: Chunk[Byte]
  )( implicit
    tagged: Tagged[A]
  ) =
    if (from.isEmpty) ZIO.fail(DeserializationError("Empty chunk."))
    else {
      val (tag, chunk) = from.splitAt(1)
      for {
        codec <- tagged.codecFor(tag(0))
        data  <- codec.fromChunk(chunk)
      } yield data
    }

  private[hyparview] def write[A](
    data: A
  ) ( implicit
    tagged: Tagged[A]
  ) = {
    val tag = tagged.tagOf(data)
    for {
      codec <- tagged.codecFor(tag)
      chunk <- codec.toChunk(data).map(Chunk.single(tag) ++ _)
    } yield chunk
  }

}

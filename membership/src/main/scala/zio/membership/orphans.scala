package zio.membership

import zio.Chunk
import upickle.default._

import scala.reflect.ClassTag

object orphans {

  implicit def chunkReadWriter[A: ClassTag](implicit ev: ReadWriter[Array[A]]): ReadWriter[Chunk[A]] =
    ev.bimap[Chunk[A]](_.toArray, Chunk.fromArray)

  implicit def tailReadWriter[A: ReadWriter]: ReadWriter[::[A]] =
    implicitly[ReadWriter[List[A]]].bimap[::[A]](
      identity, {
        case x :: xs => ::(x, xs)
        case _       => throw new IllegalStateException("empty list")
      }
    )

}

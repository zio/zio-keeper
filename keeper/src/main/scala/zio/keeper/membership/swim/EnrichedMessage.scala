package zio.keeper.membership.swim

import upickle.default._
import zio.keeper.SerializationError._
import zio.keeper.{ ByteCodec, TaggedCodec }
import zio.{ Chunk, IO, ZIO }

case class EnrichedMessage[A, B](msg: A, piggyBaked: List[B])

object EnrichedMessage {

  implicit def codec[A: TaggedCodec, B: TaggedCodec]: ByteCodec[EnrichedMessage[A, B]] =
    new ByteCodec[EnrichedMessage[A, B]] {

      override def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, EnrichedMessage[A, B]] =
        ZIO
          .effect(readBinary[(Array[Byte], List[Array[Byte]])](chunk.toArray))
          .mapError(DeserializationTypeError(_))
          .flatMap {
            case (b1, b2s) =>
              TaggedCodec.read[A](Chunk.fromArray(b1)) <*>
                ZIO.foreach(b2s)(b2 => TaggedCodec.read[B](Chunk.fromArray(b2)))
          }
          .map(ab => EnrichedMessage(ab._1, ab._2))

      override def toChunk(a: EnrichedMessage[A, B]): IO[SerializationTypeError, Chunk[Byte]] =
        TaggedCodec
          .write[A](a.msg)
          .zip(
            ZIO.foreach(a.piggyBaked)(b => TaggedCodec.write(b))
          )
          .flatMap {
            case (ch1, ch2s) =>
              ZIO
                .effect(
                  Chunk.fromArray(
                    writeBinary[(Array[Byte], List[Array[Byte]])]((ch1.toArray, ch2s.map(_.toArray)))
                  )
                )
                .mapError(SerializationTypeError(_))
          }
    }

}

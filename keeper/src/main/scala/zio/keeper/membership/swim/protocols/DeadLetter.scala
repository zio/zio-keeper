package zio.keeper.membership.swim.protocols

import zio.keeper.membership.NodeAddress
import zio.keeper.membership.swim.Protocol
import zio.keeper.{ByteCodec, SerializationError, TaggedCodec}
import zio.logging.slf4j._
import zio.stream.ZStream
import zio.{Chunk, IO, ZIO}

object DeadLetter {

  private implicit val deadLetterCodec: TaggedCodec[Chunk[Byte]] = TaggedCodec.instance(
    _ => 100,
    {
      case _ => new ByteCodec[Chunk[Byte]] {
        override def fromChunk(chunk: Chunk[Byte]): IO[SerializationError.DeserializationTypeError, Chunk[Byte]] =
          ZIO.succeed(chunk)

        override def toChunk(a: Chunk[Byte]): IO[SerializationError.SerializationTypeError, Chunk[Byte]] =
          ZIO.succeed(a)
      }
    }

  )

  def protocol =
    Protocol[NodeAddress, Chunk[Byte]].apply(
      {
        case (sender, _) =>
          logger.error("message from: " + sender + " in dead letter")
            .as(None)
      },
      ZStream.empty
    )


}

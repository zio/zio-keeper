package zio.keeper

import zio.{ Chunk, IO, ZIO }
import zio.keeper.SerializationError.DeserializationTypeError

object encoding {

  def byteChunkToInt(b: Chunk[Byte]): IO[DeserializationTypeError, Int] =
    ZIO
      .effect {
        BigInt.apply(b.toArray).intValue
      }
      .mapError(DeserializationTypeError(_))

  def intToByteChunk(a: Int): Chunk[Byte] =
    Chunk(((a >> 24) & 0xFF).toByte, ((a >> 16) & 0xFF).toByte, ((a >> 8) & 0xFF).toByte, (a & 0xFF).toByte)

}

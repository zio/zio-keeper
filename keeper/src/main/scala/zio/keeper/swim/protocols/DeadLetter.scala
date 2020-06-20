package zio.keeper.swim.protocols

import zio.{ Chunk, ZIO, keeper }
import zio.keeper.swim.{ Message, Protocol }
import zio.stream.ZStream
import zio.logging._

object DeadLetter {

  def protocol: ZIO[Logging, keeper.Error, Protocol[Chunk[Byte]]] =
    Protocol[Chunk[Byte]].make(
      { msg =>
        log(LogLevel.Error)("message [" + msg + "] in dead letter") *> Message.noResponse
      },
      ZStream.empty
    )

}

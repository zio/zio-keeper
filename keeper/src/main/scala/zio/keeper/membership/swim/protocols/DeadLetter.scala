package zio.keeper.membership.swim.protocols

import zio.Chunk
import zio.keeper.membership.swim.{ Message, Protocol }
import zio.stream.ZStream
import zio.logging._

object DeadLetter {

  def protocol =
    Protocol[Chunk[Byte]].make(
      { msg =>
        log(LogLevel.Error)("message [" + msg + "] in dead letter") *> Message.noResponse
      },
      ZStream.empty
    )

}

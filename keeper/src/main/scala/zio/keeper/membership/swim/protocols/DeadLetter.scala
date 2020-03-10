package zio.keeper.membership.swim.protocols

import zio.Chunk
import zio.keeper.membership.swim.Protocol
import zio.logging.slf4j._
import zio.stream.ZStream

object DeadLetter {

  def protocol =
    Protocol[Chunk[Byte]].apply(
      {
        msg =>
          logger
            .error("message [" + msg + "] in dead letter")
            .as(None)
      },
      ZStream.empty
    )

}

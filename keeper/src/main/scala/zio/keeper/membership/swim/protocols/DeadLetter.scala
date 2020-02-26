package zio.keeper.membership.swim.protocols

import zio.Chunk
import zio.keeper.membership.swim.{ NodeId, Protocol }
import zio.logging.slf4j._
import zio.stream.ZStream

object DeadLetter {

  def protocol =
    Protocol[NodeId, Chunk[Byte]].apply(
      {
        case (sender, _) =>
          logger
            .error("message from: " + sender + " in dead letter")
            .as(None)
      },
      ZStream.empty
    )

}

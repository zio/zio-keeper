package zio.keeper

import zio.keeper.membership.hyparview.{ ActiveProtocol, InitialProtocol, NeighborReply }
import zio.test._

object TaggedCodecSpec extends DefaultRunnableSpec {

  def spec =
    suite("TaggedCodec")(
      TaggedCodecLaws[ActiveProtocol](gens.activeProtocol),
      TaggedCodecLaws[InitialProtocol](gens.initialProtocol),
      TaggedCodecLaws[NeighborReply](gens.neighborReply)
    )
}

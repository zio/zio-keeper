package zio.membership

import zio.keeper.membership.TaggedCodecLaws
import zio.membership.hyparview.ActiveProtocol._
import zio.membership.hyparview.{ ActiveProtocol, InitialProtocol, NeighborReply }
import zio.test.{ Gen, _ }

object TaggedCodecSpec extends DefaultRunnableSpec {

  def spec =
    suite("TaggedCodec")(
      TaggedCodecLaws[ActiveProtocol[Int]](gens.activeProtocol(Gen.anyInt)),
      TaggedCodecLaws[InitialProtocol[Int]](gens.initialProtocol(Gen.anyInt)),
      TaggedCodecLaws[NeighborReply](gens.neighborReply)
    )
}

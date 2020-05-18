package zio.keeper

import zio.keeper.membership.hyparview.ActiveProtocol
import zio.keeper.membership.hyparview.ActiveProtocol._
import zio.keeper.membership.hyparview.InitialProtocol
import zio.keeper.membership.hyparview.InitialProtocol._
import zio.keeper.membership.hyparview.JoinReply
import zio.keeper.membership.hyparview.NeighborReply
import zio.keeper.membership.hyparview.NeighborReply._
import zio.test._

object ByteCodecSpec extends DefaultRunnableSpec {

  def spec =
    suite("ByteCodec")(
      ByteCodecLaws[JoinReply](gens.joinReply),
      // initial protocol
      ByteCodecLaws[Join](gens.join),
      ByteCodecLaws[ShuffleReply](gens.shuffleReply),
      ByteCodecLaws[Neighbor](gens.neighbor),
      ByteCodecLaws[ForwardJoinReply](gens.forwardJoinReply),
      ByteCodecLaws[InitialProtocol](gens.initialProtocol),
      // neighborreply
      ByteCodecLaws[Accept.type](gens.accept),
      ByteCodecLaws[Reject.type](gens.reject),
      ByteCodecLaws[NeighborReply](gens.neighborReply),
      // active protocol
      ByteCodecLaws[Disconnect](gens.disconnect),
      ByteCodecLaws[ForwardJoin](gens.forwardJoin),
      ByteCodecLaws[Shuffle](gens.shuffle),
      ByteCodecLaws[ActiveProtocol](gens.activeProtocol),
      // plumtree messages
      ByteCodecLaws[Prune.type](gens.prune),
      ByteCodecLaws[IHave](gens.iHave),
      ByteCodecLaws[Graft](gens.graft),
      ByteCodecLaws[UserMessage](gens.userMessage),
      ByteCodecLaws[Gossip](gens.gossip)
    )
}

package zio.membership

import zio.membership.hyparview.ActiveProtocol._
import zio.membership.hyparview.InitialProtocol.{ ForwardJoinReply, Join, Neighbor, ShuffleReply }
import zio.membership.hyparview.NeighborReply.{ Accept, Reject }
import zio.membership.hyparview.JoinReply
import zio.keeper.membership.ByteCodecLaws
import zio.test._
import zio.test.Gen

object ByteCodecSpec extends DefaultRunnableSpec {

  def spec =
    suite("ByteCodec")(
      ByteCodecLaws[JoinReply[Int]](gens.joinReply(Gen.anyInt)),
      // initial protocol
      ByteCodecLaws[Join[Int]](gens.join(Gen.anyInt)),
      ByteCodecLaws[ShuffleReply[Int]](gens.shuffleReply(Gen.anyInt)),
      ByteCodecLaws[Neighbor[Int]](gens.neighbor(Gen.anyInt)),
      ByteCodecLaws[ForwardJoinReply[Int]](gens.forwardJoinReply(Gen.anyInt)),
      // neighborreply
      ByteCodecLaws[Accept.type](gens.accept),
      ByteCodecLaws[Reject.type](gens.reject),
      // active protocol
      ByteCodecLaws[Disconnect[Int]](gens.disconnect(Gen.anyInt)),
      ByteCodecLaws[ForwardJoin[Int]](gens.forwardJoin(Gen.anyInt)),
      ByteCodecLaws[Shuffle[Int]](gens.shuffle(Gen.anyInt)),
      // plumtree messages
      ByteCodecLaws[Prune.type](gens.prune),
      ByteCodecLaws[IHave](gens.iHave),
      ByteCodecLaws[Graft](gens.graft),
      ByteCodecLaws[UserMessage](gens.userMessage),
      ByteCodecLaws[Gossip](gens.gossip)
    )
}

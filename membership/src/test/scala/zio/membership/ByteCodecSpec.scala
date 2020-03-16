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
      ByteCodecLaws[JoinReply[Int]](CustomGen.joinReply(Gen.anyInt)),
      // initial protocol
      ByteCodecLaws[Join[Int]](CustomGen.join(Gen.anyInt)),
      ByteCodecLaws[ShuffleReply[Int]](CustomGen.shuffleReply(Gen.anyInt)),
      ByteCodecLaws[Neighbor[Int]](CustomGen.neighbor(Gen.anyInt)),
      ByteCodecLaws[ForwardJoinReply[Int]](CustomGen.forwardJoinReply(Gen.anyInt)),
      // neighborreply
      ByteCodecLaws[Accept.type](CustomGen.accept),
      ByteCodecLaws[Reject.type](CustomGen.reject),
      // active protocol
      ByteCodecLaws[Disconnect[Int]](CustomGen.disconnect(Gen.anyInt)),
      ByteCodecLaws[ForwardJoin[Int]](CustomGen.forwardJoin(Gen.anyInt)),
      ByteCodecLaws[Shuffle[Int]](CustomGen.shuffle(Gen.anyInt)),
      // plumtree messages
      ByteCodecLaws[Prune.type](CustomGen.prune),
      ByteCodecLaws[IHave](CustomGen.iHave),
      ByteCodecLaws[Graft](CustomGen.graft),
      ByteCodecLaws[UserMessage](CustomGen.userMessage),
      ByteCodecLaws[Gossip](CustomGen.gossip)
    )
}

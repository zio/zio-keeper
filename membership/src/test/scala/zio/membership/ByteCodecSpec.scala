package zio.membership

import zio.membership.hyparview.ActiveProtocol._
import zio.membership.hyparview.InitialMessage.{ Join, ShuffleReply }
import zio.membership.hyparview.NeighborReply.{ Accept, Reject }
import zio.membership.hyparview.{ JoinReply, Neighbor }
import zio.test._
import zio.test.Gen

object ByteCodecSpec
    extends DefaultRunnableSpec(
      suite("ByteCodecSpec")(
        ByteCodecLaws[Disconnect[Int]](CustomGen.disconnect(Gen.anyInt)),
        ByteCodecLaws[ForwardJoin[Int]](CustomGen.forwardJoin(Gen.anyInt)),
        ByteCodecLaws[Shuffle[Int]](CustomGen.shuffle(Gen.anyInt)),
        ByteCodecLaws[UserMessage](CustomGen.userMessage),
        ByteCodecLaws[Neighbor[Int]](CustomGen.neighbor(Gen.anyInt)),
        ByteCodecLaws[Join[Int]](CustomGen.join(Gen.anyInt)),
        ByteCodecLaws[ShuffleReply[Int]](CustomGen.shuffleReply(Gen.anyInt)),
        ByteCodecLaws[JoinReply[Int]](CustomGen.joinReply(Gen.anyInt)),
        ByteCodecLaws[Accept.type](CustomGen.accept),
        ByteCodecLaws[Reject.type](CustomGen.reject)
      )
    )

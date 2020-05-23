package zio.keeper

import zio.keeper.membership.hyparview.ActiveProtocol
import zio.keeper.membership.hyparview.ActiveProtocol._
import zio.keeper.membership.hyparview.InitialProtocol
import zio.keeper.membership.hyparview.InitialProtocol._
import zio.keeper.membership.hyparview.JoinReply
import zio.keeper.membership.hyparview.NeighborReply
import zio.keeper.membership.hyparview.NeighborReply._
import zio.keeper.membership.swim.protocols.{ FailureDetection, Initial, Suspicion }
import zio.keeper.membership.swim.protocols.FailureDetection.{ Ack, Nack, Ping, PingReq }
import zio.keeper.membership.swim.protocols.Suspicion.{ Alive, Dead, Suspect }
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
      ByteCodecLaws[Gossip](gens.gossip),
      //swim failure detection
      ByteCodecLaws[Ping.type](gens.ping),
      ByteCodecLaws[Ack.type](gens.ack),
      ByteCodecLaws[Nack.type](gens.nack),
      ByteCodecLaws[PingReq](gens.pingReq),
      ByteCodecLaws[FailureDetection](gens.failureDetectionProtocol),
      //swim suspicion
      ByteCodecLaws[Suspect](gens.suspect),
      ByteCodecLaws[Alive](gens.alive),
      ByteCodecLaws[Dead](gens.dead),
      ByteCodecLaws[Suspicion](gens.suspicionProtocol),
      //swim initial
      ByteCodecLaws[Initial.Join](gens.swimJoin),
      ByteCodecLaws[Initial.Accept.type](gens.swimAccept),
      ByteCodecLaws[Initial.Reject](gens.swimReject),
      ByteCodecLaws[Initial](gens.initialSwimlProtocol)
    )
}

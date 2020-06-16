package zio.keeper

import zio.keeper.hyparview.ActiveProtocol
import zio.keeper.hyparview.ActiveProtocol._
import zio.keeper.hyparview.InitialProtocol
import zio.keeper.hyparview.InitialProtocol._
import zio.keeper.hyparview.JoinReply
import zio.keeper.hyparview.NeighborReply
import zio.keeper.hyparview.NeighborReply._
import zio.keeper.swim.protocols.{ FailureDetection, Initial }
import zio.keeper.swim.protocols.FailureDetection.{ Ack, Alive, Dead, Nack, Ping, PingReq, Suspect }
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
      //swim initial
      ByteCodecLaws[Initial.Join](gens.swimJoin),
      ByteCodecLaws[Initial.Accept.type](gens.swimAccept),
      ByteCodecLaws[Initial.Reject](gens.swimReject),
      ByteCodecLaws[Initial](gens.initialSwimlProtocol)
    )
}

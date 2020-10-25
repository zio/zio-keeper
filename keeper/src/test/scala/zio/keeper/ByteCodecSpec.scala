package zio.keeper

import zio.keeper.hyparview.Message
import zio.keeper.hyparview.Message._
import zio.keeper.hyparview.Message.PeerMessage._
import zio.keeper.swim.protocols.{ FailureDetection, Initial }
import zio.keeper.swim.protocols.FailureDetection.{ Ack, Alive, Dead, Nack, Ping, PingReq, Suspect }
import zio.test._

object ByteCodecSpec extends KeeperSpec {

  def spec =
    suite("ByteCodec")(
      // hyparview messages
      ByteCodecLaws[Message](gens.hyparview.message),
      ByteCodecLaws[Disconnect](gens.hyparview.disconnect),
      ByteCodecLaws[ForwardJoin](gens.hyparview.forwardJoin),
      ByteCodecLaws[ForwardJoinReply](gens.hyparview.forwardJoinReply),
      ByteCodecLaws[Join](gens.hyparview.join),
      ByteCodecLaws[Neighbor](gens.hyparview.neighbor),
      ByteCodecLaws[NeighborAccept.type](gens.hyparview.neighborAccept),
      ByteCodecLaws[NeighborReject.type](gens.hyparview.neighborReject),
      ByteCodecLaws[Shuffle](gens.hyparview.shuffle),
      ByteCodecLaws[ShuffleReply](gens.hyparview.shuffleReply),
      // plumtree messages
      ByteCodecLaws[Prune.type](gens.hyparview.prune),
      ByteCodecLaws[IHave](gens.hyparview.iHave),
      ByteCodecLaws[Graft](gens.hyparview.graft),
      ByteCodecLaws[UserMessage](gens.hyparview.userMessage),
      ByteCodecLaws[Gossip](gens.hyparview.gossip),
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

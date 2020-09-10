package zio.keeper

import java.util.UUID

import zio.Chunk
import zio.keeper.uuid.makeRandomUUID
import zio.keeper.swim.protocols.{ FailureDetection, Initial }
import zio.keeper.swim.protocols.FailureDetection.{ Ack, Alive, Dead, Nack, Ping, PingReq, Suspect }
import zio.random.Random
import zio.test._

object gens {

  object hyparview {
    import zio.keeper.hyparview._
    import zio.keeper.hyparview.Message._
    import zio.keeper.hyparview.Message.PeerMessage._

    val timeToLive: Gen[Random, TimeToLive] =
      Gen.anyInt.map(TimeToLive.apply)

    val round: Gen[Random, Round] =
      Gen.int(0, 128).map { n =>
        def go(round: Round, remaining: Int): Round =
          if (remaining <= 0) round
          else go(round.inc, remaining - 1)
        go(Round.zero, n)
      }

    val joinReply: Gen[Random with Sized, JoinReply] =
      nodeAddress.map(JoinReply.apply)

    val join: Gen[Random with Sized, Join] =
      nodeAddress.map(Join.apply)

    val shuffleReply: Gen[Random with Sized, ShuffleReply] =
      for {
        passiveNodes   <- Gen.listOf(nodeAddress)
        sentOriginally <- Gen.listOf(nodeAddress)
      } yield ShuffleReply(passiveNodes, sentOriginally)

    val neighbor: Gen[Random with Sized, Neighbor] =
      for {
        sender         <- nodeAddress
        isHighPriority <- Gen.boolean
      } yield Neighbor(sender, isHighPriority)

    val forwardJoinReply: Gen[Random with Sized, ForwardJoinReply] =
      for {
        sender <- nodeAddress
      } yield ForwardJoinReply(sender)

    val neighborAccept: Gen[Random, NeighborAccept.type] =
      Gen.const(NeighborAccept)

    val neighborReject: Gen[Random, NeighborReject.type] =
      Gen.const(NeighborReject)

    val disconnect: Gen[Random with Sized, Disconnect] =
      for {
        alive <- Gen.boolean
      } yield Disconnect(alive)

    val forwardJoin: Gen[Random with Sized, ForwardJoin] =
      for {
        originalSender <- nodeAddress
        ttl            <- timeToLive
      } yield ForwardJoin(originalSender, ttl)

    def shuffle: Gen[Random with Sized, Shuffle] =
      for {
        sender         <- nodeAddress
        originalSender <- nodeAddress
        activeNodes    <- Gen.listOf(nodeAddress)
        passiveNodes   <- Gen.listOf(nodeAddress)
        ttl            <- timeToLive
      } yield Shuffle(sender, originalSender, activeNodes, passiveNodes, ttl)

    val prune: Gen[Any, Prune.type] =
      Gen.const(Prune)

    val iHave: Gen[Random with Sized, IHave] =
      Gen.listOf(uuid.zip(round)).map(entries => IHave.apply(Chunk.fromIterable(entries)))

    val graft: Gen[Any, Graft] =
      uuid.map(Graft(_))

    val userMessage: Gen[Random with Sized, UserMessage] =
      Gen.listOf(Gen.anyByte).map(bytes => UserMessage(Chunk.fromIterable(bytes)))

    val gossip: Gen[Random with Sized, Gossip] =
      for {
        body  <- Gen.listOf(Gen.anyByte).map(Chunk.fromIterable)
        uuid  <- uuid
        round <- round
      } yield Gossip(uuid, body, round)

    val peerMessage: Gen[Random with Sized, PeerMessage] =
      Gen.oneOf(prune, iHave, graft, userMessage, gossip)

    val message: Gen[Random with Sized, Message] =
      Gen.oneOf(
        disconnect,
        forwardJoin,
        forwardJoinReply,
        join,
        joinReply,
        neighbor,
        neighborAccept,
        neighborReject,
        peerMessage,
        shuffle,
        shuffleReply
      )

  }

  val nodeAddress: Gen[Random with Sized, NodeAddress] =
    Gen.chunkOf(Gen.anyByte).zipWith(Gen.anyInt)(NodeAddress.apply)

  val uuid: Gen[Any, UUID] =
    Gen.fromEffect(makeRandomUUID)

  val ping: Gen[Any, Ping.type] =
    Gen.const(Ping)

  val ack: Gen[Any, Ack.type] =
    Gen.const(Ack)

  val nack: Gen[Any, Nack.type] =
    Gen.const(Nack)

  val pingReq: Gen[Random with Sized, PingReq] =
    nodeAddress.map(PingReq(_))

  val suspect: Gen[Random with Sized, Suspect] =
    nodeAddress.zip(nodeAddress).map { case (from, to) => Suspect(from, to) }

  val alive: Gen[Random with Sized, Alive] =
    nodeAddress.map(Alive)

  val dead: Gen[Random with Sized, Dead] =
    nodeAddress.map(Dead)

  val failureDetectionProtocol: Gen[Random with Sized, FailureDetection] =
    Gen.oneOf(ping, ack, nack, pingReq, suspect, alive, dead)

  val swimJoin: Gen[Random with Sized, Initial.Join] =
    nodeAddress.map(Initial.Join)

  val swimAccept: Gen[Random with Sized, Initial.Accept.type] =
    Gen.const(Initial.Accept)

  val swimReject: Gen[Random with Sized, Initial.Reject] =
    Gen.alphaNumericString.map(Initial.Reject)

  val initialSwimlProtocol: Gen[Random with Sized, Initial] =
    Gen.oneOf(swimReject, swimAccept, swimJoin)
}

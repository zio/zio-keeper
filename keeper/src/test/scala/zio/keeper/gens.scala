package zio.keeper

import java.util.UUID

import zio.Chunk
import zio.keeper.membership.hyparview.ActiveProtocol._
import zio.keeper.membership.hyparview.InitialProtocol._
import zio.keeper.membership.hyparview.NeighborReply._
import zio.keeper.membership.hyparview._
import zio.keeper.membership.swim.protocols.{ FailureDetection, Initial, Suspicion }
import zio.keeper.membership.swim.protocols.FailureDetection.{ Ack, Nack, Ping, PingReq }
import zio.keeper.membership.swim.protocols.Suspicion.{ Alive, Dead, Suspect }
import zio.random.Random
import zio.test._

object gens {

  val timeToLive: Gen[Random, TimeToLive] =
    Gen.anyInt.map(TimeToLive.apply)

  val round: Gen[Random, Round] =
    Gen.int(0, 128).map { n =>
      def go(round: Round, remaining: Int): Round =
        if (remaining <= 0) round
        else go(round.inc, remaining - 1)
      go(Round.zero, n)
    }

  val nodeAddress: Gen[Random with Sized, NodeAddress] =
    Gen.listOf(Gen.anyByte).map(_.toArray).zipWith(Gen.anyInt)(NodeAddress.apply)

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

  val initialProtocol: Gen[Random with Sized, InitialProtocol] =
    Gen.oneOf(join, shuffleReply, neighbor, forwardJoinReply)

  val accept: Gen[Any, Accept.type] = Gen.const(Accept)

  val reject: Gen[Any, Reject.type] = Gen.const(Reject)

  val neighborReply: Gen[Random, NeighborReply] =
    Gen.oneOf(accept, reject)

  val disconnect: Gen[Random with Sized, Disconnect] =
    for {
      sender <- nodeAddress
      alive  <- Gen.boolean
    } yield Disconnect(sender, alive)

  val forwardJoin: Gen[Random with Sized, ForwardJoin] =
    for {
      sender         <- nodeAddress
      originalSender <- nodeAddress
      ttl            <- gens.timeToLive
    } yield ForwardJoin(sender, originalSender, ttl)

  def shuffle: Gen[Random with Sized, Shuffle] =
    for {
      sender         <- nodeAddress
      originalSender <- nodeAddress
      activeNodes    <- Gen.listOf(nodeAddress)
      passiveNodes   <- Gen.listOf(nodeAddress)
      ttl            <- gens.timeToLive
    } yield Shuffle(sender, originalSender, activeNodes, passiveNodes, ttl)

  val uuid: Gen[Any, UUID] =
    Gen.fromEffect(makeRandomUUID)

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

  val plumTreeProtocol: Gen[Random with Sized, PlumTreeProtocol] =
    Gen.oneOf(prune, iHave, graft, userMessage, gossip)

  val activeProtocol: Gen[Random with Sized, ActiveProtocol] =
    Gen.oneOf(disconnect, forwardJoin, shuffle, plumTreeProtocol)

  val ping: Gen[Any, Ping.type] =
    Gen.const(Ping)

  val ack: Gen[Any, Ack.type] =
    Gen.const(Ack)

  val nack: Gen[Any, Nack.type] =
    Gen.const(Nack)

  val pingReq: Gen[Random with Sized, PingReq] =
    nodeAddress.map(PingReq(_))

  val failureDetectionProtocol: Gen[Random with Sized, FailureDetection] =
    Gen.oneOf(ping, ack, nack, pingReq)

  val suspect: Gen[Random with Sized, Suspect] =
    nodeAddress.zip(nodeAddress).map { case (from, to) => Suspect(from, to) }

  val alive: Gen[Random with Sized, Alive] =
    nodeAddress.map(Alive)

  val dead: Gen[Random with Sized, Dead] =
    nodeAddress.map(Dead)

  val suspicionProtocol: Gen[Random with Sized, Suspicion] =
    Gen.oneOf(suspect, alive, dead)

  val swimJoin: Gen[Random with Sized, Initial.Join] =
    nodeAddress.map(Initial.Join)

  val swimAccept: Gen[Random with Sized, Initial.Accept.type] =
    Gen.const(Initial.Accept)

  val swimReject: Gen[Random with Sized, Initial.Reject] =
    Gen.alphaNumericString.map(Initial.Reject)

  val initialSwimlProtocol: Gen[Random with Sized, Initial] =
    Gen.oneOf(swimReject, swimAccept, swimJoin)
}

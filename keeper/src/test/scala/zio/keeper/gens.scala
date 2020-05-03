package zio.keeper

import java.util.UUID

import zio.Chunk
import zio.keeper.membership.hyparview.ActiveProtocol._
import zio.keeper.membership.hyparview.InitialProtocol._
import zio.keeper.membership.hyparview.NeighborReply._
import zio.keeper.membership.hyparview._
import zio.random.Random
import zio.test._

object gens {

  val timeToLive: Gen[Random, TimeToLive] =
    Gen.anyInt.map(TimeToLive.apply)

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
    Gen.listOf(uuid).flatMap {
      case Nil     => Gen.empty
      case x :: xs => Gen.const(IHave(::(x, xs)))
    }

  val graft: Gen[Any, Graft] =
    uuid.map(Graft(_))

  val userMessage: Gen[Random with Sized, UserMessage] =
    Gen.listOf(Gen.anyByte).map(bytes => UserMessage(Chunk.fromIterable(bytes)))

  val gossip: Gen[Random with Sized, Gossip] =
    for {
      body <- Gen.listOf(Gen.anyByte).map(Chunk.fromIterable)
      uuid <- uuid
    } yield Gossip(uuid, body)

  val plumTreeProtocol: Gen[Random with Sized, PlumTreeProtocol] =
    Gen.oneOf(prune, iHave, graft, userMessage, gossip)

  val activeProtocol: Gen[Random with Sized, ActiveProtocol] =
    Gen.oneOf(disconnect, forwardJoin, shuffle, plumTreeProtocol)
}

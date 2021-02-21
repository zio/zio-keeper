package zio.keeper

import java.util.UUID

import zio.Chunk
import zio.keeper.uuid.makeRandomUUID
import zio.random.Random
import zio.test._

object gens {

  object hyparview {
    import zio.keeper.hyparview._
    import zio.keeper.hyparview.Message._

    val timeToLive: Gen[Random, TimeToLive] =
      Gen.anyInt.map(TimeToLive.apply)

    val round: Gen[Random, Round] =
      Gen.int(0, 128).map { n =>
        def go(round: Round, remaining: Int): Round =
          if (remaining <= 0) round
          else go(round.inc, remaining - 1)
        go(Round.zero, n)
      }

    val join: Gen[Random with Sized, Join] =
      nodeAddress.map(Join.apply)

    val shuffleReply: Gen[Random with Sized, ShuffleReply] =
      for {
        passiveNodes   <- Gen.setOf(nodeAddress)
        sentOriginally <- Gen.setOf(nodeAddress)
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
        activeNodes    <- Gen.setOf(nodeAddress)
        passiveNodes   <- Gen.setOf(nodeAddress)
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
        neighbor,
        neighborAccept,
        neighborReject,
        peerMessage,
        shuffle,
        shuffleReply
      )

  }

  val nodeAddress: Gen[Random with Sized, NodeAddress] =
    Gen.chunkOfN(4)(Gen.anyByte).zipWith(Gen.anyInt)(NodeAddress.apply)

  val uuid: Gen[Any, UUID] =
    Gen.fromEffect(makeRandomUUID)

}

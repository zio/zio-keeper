package zio.membership

import java.util.UUID

import zio.Chunk
import zio.membership.hyparview.ActiveProtocol._
import zio.membership.hyparview.InitialProtocol.{ ForwardJoinReply, Join, Neighbor, ShuffleReply }
import zio.membership.hyparview.NeighborReply.{ Accept, Reject }
import zio.membership.hyparview._
import zio.random.Random
import zio.test._

object CustomGen {

  val timeToLive: Gen[Random, TimeToLive] =
    Gen.anyInt.map(TimeToLive.apply)

  def joinReply[R <: Random, A](gen: Gen[R, A]): Gen[R, JoinReply[A]] =
    gen.map(JoinReply.apply)

  def join[R <: Random, A](gen: Gen[R, A]): Gen[R, Join[A]] =
    gen.map(Join.apply)

  def shuffleReply[R <: Random with Sized, A](
    gen: Gen[R, A]
  ): Gen[R, ShuffleReply[A]] =
    for {
      passiveNodes   <- Gen.listOf(gen)
      sentOriginally <- Gen.listOf(gen)
    } yield ShuffleReply(passiveNodes, sentOriginally)

  def neighbor[R <: Random, A](gen: Gen[R, A]): Gen[R, Neighbor[A]] =
    for {
      sender         <- gen
      isHighPriority <- Gen.boolean
    } yield Neighbor(sender, isHighPriority)

  def forwardJoinReply[R <: Random, A](gen: Gen[R, A]): Gen[R, ForwardJoinReply[A]] =
    for {
      sender <- gen
    } yield ForwardJoinReply(sender)

  def initialProtocol[R <: Random with Sized, A](gen: Gen[R, A]): Gen[R, InitialProtocol[A]] =
    Gen.oneOf(join(gen), shuffleReply(gen), neighbor(gen), forwardJoinReply(gen))

  val accept: Gen[Any, Accept.type] = Gen.const(Accept)

  val reject: Gen[Any, Reject.type] = Gen.const(Reject)

  val neighborReply: Gen[Random, NeighborReply] =
    Gen.oneOf(accept, reject)

  def disconnect[R <: Random, A](gen: Gen[R, A]): Gen[R, Disconnect[A]] =
    for {
      sender <- gen
      alive  <- Gen.boolean
    } yield Disconnect(sender, alive)

  def forwardJoin[R <: Random, A](gen: Gen[R, A]): Gen[R, ForwardJoin[A]] =
    for {
      sender         <- gen
      originalSender <- gen
      ttl            <- CustomGen.timeToLive
    } yield ForwardJoin(sender, originalSender, ttl)

  def shuffle[R <: Random with Sized, A](gen: Gen[R, A]): Gen[R, Shuffle[A]] =
    for {
      sender         <- gen
      originalSender <- gen
      activeNodes    <- Gen.listOf(gen)
      passiveNodes   <- Gen.listOf(gen)
      ttl            <- CustomGen.timeToLive
    } yield Shuffle(sender, originalSender, activeNodes, passiveNodes, ttl)

  def uuid: Gen[Any, UUID] =
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

  def plumTreeProtocol: Gen[Random with Sized, PlumTreeProtocol] =
    Gen.oneOf(prune, iHave, graft, userMessage, gossip)

  def activeProtocol[R <: Random with Sized, A](gen: Gen[R, A]): Gen[R, ActiveProtocol[A]] =
    Gen.oneOf(disconnect(gen), forwardJoin(gen), shuffle(gen), plumTreeProtocol)
}

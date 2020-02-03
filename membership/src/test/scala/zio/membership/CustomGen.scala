package zio.membership

import zio.Chunk
import zio.membership.hyparview.ActiveProtocol._
import zio.membership.hyparview.InitialMessage.{ Join, ShuffleReply }
import zio.membership.hyparview.NeighborReply.{ Accept, Reject }
import zio.membership.hyparview._
import zio.random.Random
import zio.test._

object CustomGen {

  val timeToLive: Gen[Random, TimeToLive] =
    Gen.anyInt.map(TimeToLive.apply)

  def disconnect[R <: Random, A](gen: Gen[R, A]): Gen[R, Disconnect[A]] =
    for {
      sender <- gen
      alive  <- Gen.boolean
    } yield Disconnect(sender, alive)

  def join[R <: Random, A](gen: Gen[R, A]): Gen[R, Join[A]] =
    gen.map(Join.apply)

  def joinReply[R <: Random, A](gen: Gen[R, A]): Gen[R, JoinReply[A]] =
    gen.map(JoinReply.apply)

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

  def shuffleReply[R <: Random with Sized, A](
    gen: Gen[R, A]
  ): Gen[R, ShuffleReply[A]] =
    for {
      passiveNodes   <- Gen.listOf(gen)
      sentOriginally <- Gen.listOf(gen)
    } yield ShuffleReply(passiveNodes, sentOriginally)

  val userMessage: Gen[Random with Sized, UserMessage] =
    Gen.listOf(Gen.anyByte).map(bytes => UserMessage(Chunk.fromIterable(bytes)))

  def neighbor[R <: Random, A](gen: Gen[R, A]): Gen[R, Neighbor[A]] =
    for {
      sender         <- gen
      isHighPriority <- Gen.boolean
    } yield Neighbor(sender, isHighPriority)

  val reject: Gen[Any, Reject.type] = Gen.const(Reject)

  val accept: Gen[Any, Accept.type] = Gen.const(Accept)
}

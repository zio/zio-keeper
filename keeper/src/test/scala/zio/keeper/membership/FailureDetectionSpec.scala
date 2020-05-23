package zio.keeper.membership

import izumi.reflect.Tags.Tag
import zio._
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.keeper.NodeAddress
import zio.keeper.membership.ProtocolRecorder.ProtocolRecorder
import zio.keeper.membership.swim.Nodes._
import zio.keeper.membership.swim.protocols.FailureDetection
import zio.keeper.membership.swim.protocols.FailureDetection.{Ack, Ping, PingReq}
import zio.keeper.membership.swim.{ConversationId, Message, Nodes, Protocol}
import zio.logging.{Logging, log}
import zio.stream.{Sink, ZStream}
import zio.test.Assertion._
import zio.test.environment.TestClock
import zio.test.{assert, _}

object FailureDetectionSpec extends DefaultRunnableSpec {

  val logger     = Logging.console((_, line) => line)
  val nodesLayer = (ZLayer.requires[Clock] ++ logger) >>> Nodes.live

  val recorder: ZLayer[Clock with Console, Nothing, ProtocolRecorder[FailureDetection]] =
    (ZLayer.requires[Clock] ++ nodesLayer ++ logger ++ ConversationId.live) >>>
      ProtocolRecorder
        .make(
          FailureDetection
            .protocol(1.second, 500.milliseconds)
        )
        .orDie

  val testLayer = ConversationId.live ++ logger ++ nodesLayer ++ recorder

  val nodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
  val nodeAddress2 = NodeAddress(Array(11, 22, 33, 44), 1111)
  val nodeAddress3 = NodeAddress(Array(2, 3, 4, 5), 1111)

  val spec = suite("failure detection")(
    testM("Ping healthy Nodes periodically") {
      for {
        recorder <- ProtocolRecorder[FailureDetection].flatMap(_.withBehavior {
          case Message.Direct(nodeAddr, ackId, Ping) =>
            Message.Direct(nodeAddr, ackId, Ack)
        })
        _        <- addNode(nodeAddress1)
        _        <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _        <- addNode(nodeAddress2)
        _        <- changeNodeState(nodeAddress2, NodeState.Healthy)
        _        <- addNode(nodeAddress3)
        _        <- changeNodeState(nodeAddress3, NodeState.Unreachable)
        _         <- TestClock.adjust(100.seconds)
        messages <- recorder.collectN(3){case Message.Direct(addr, _, Ping) => addr}
      } yield assert(messages.toSet)(equalTo(Set(nodeAddress2, nodeAddress1)))
    }.provideCustomLayer(testLayer),
    testM("should change to Dead if there is no nodes to send PingReq") {
      for {
        recorder  <- ProtocolRecorder[FailureDetection]
        _         <- addNode(nodeAddress1)
        _         <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _         <- TestClock.adjust(1500.milliseconds)
        messages  <- recorder.collectN(2){case msg => msg}
        nodeState <- nodeState(nodeAddress1)
      } yield assert(messages)(equalTo(List(Message.Direct(nodeAddress1, 1, Ping), Message.NoResponse))) &&
        assert(nodeState)(equalTo(NodeState.Dead))
    }.provideCustomLayer(testLayer),
    testM("should send PingReq to other node") {
      for {
        recorder <- ProtocolRecorder[FailureDetection].flatMap(_.withBehavior{
          case Message.Direct(`nodeAddress2`, ackId, Ping) =>
            Message.Direct(nodeAddress2, ackId, Ack)
          case Message.Direct(`nodeAddress1`, _, Ping) =>
            Message.NoResponse //simulate failing node
        })
        _        <- addNode(nodeAddress1)
        _        <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _        <- addNode(nodeAddress2)
        _        <- changeNodeState(nodeAddress2, NodeState.Healthy)
        _        <- TestClock.adjust(10.seconds)
        msg <- recorder.collectN(1){ case Message.Direct(_, _, msg: PingReq) => msg}
        nodeState <- nodeState(nodeAddress1)
      } yield assert(msg)(equalTo(List(PingReq(nodeAddress1)))) &&
        assert(nodeState)(equalTo(NodeState.Unreachable))
    }.provideCustomLayer(testLayer)
  )

}

object ProtocolRecorder {
  type ProtocolRecorder[A] = Has[ProtocolRecorder.Service[A]]

  trait Service[A] {
    def withBehavior(pf: PartialFunction[Message.Direct[A], Message[A]]): UIO[Service[A]]
    def collectN[B](n: Long)(pr: PartialFunction[Message[A], B]): UIO[List[B]]
    def send(msg: Message.Direct[A]): IO[zio.keeper.Error, Message[A]]
  }

  def apply[A: Tag] = ZIO.environment[ProtocolRecorder[A]].map(_.get)

  def make[R, E, A: Tag](
    protocolFactory: ZIO[R, E, Protocol[A]]
  ): ZLayer[Clock with Logging with Nodes with R, E, ProtocolRecorder[A]] =
    ZLayer.fromEffect {
      def loop(
        messageQueue: zio.Queue[Message[A]],
        message: Message[A],
        behaviorRef: Ref[PartialFunction[Message.Direct[A], Message[A]]],
        protocol: Protocol[A]
      ): ZIO[Clock with Logging with Nodes, zio.keeper.Error, Unit] =
        log.info("recorded message: " + message) *>
        (message match {
          case Message.WithTimeout(message, action, timeout) =>
            loop(messageQueue, message, behaviorRef, protocol).unit *> action.delay(timeout).flatMap(loop(messageQueue, _, behaviorRef, protocol)).fork.unit
          case md: Message.Direct[A] => messageQueue.offer(md) *> behaviorRef.get.flatMap{
            fn => ZIO.whenCase(fn.lift(md)){
              case Some(d: Message.Direct[A]) => protocol.onMessage(d)
            }
          }
          case msg =>
            messageQueue.offer(msg).unit
        })
      for {
        behaviorRef <- Ref.make[PartialFunction[Message.Direct[A], Message[A]]](PartialFunction.empty)
        protocol     <- protocolFactory
        messageQueue <- ZQueue.bounded[Message[A]](100)
        _            <- protocol.produceMessages.foreach(loop(messageQueue, _, behaviorRef, protocol)).fork
        stream = ZStream.fromQueue(messageQueue)
      } yield new Service[A] {

        override def withBehavior(pf: PartialFunction[Message.Direct[A], Message[A]]): UIO[Service[A]] =
          behaviorRef.set(pf).as(this)

        override def collectN[B](n: Long)(pf: PartialFunction[Message[A], B]): UIO[List[B]] =
          stream.collect(pf).run(Sink.collectAllN[B](n))//.timeoutFail(new RuntimeException)(2.seconds).provideLayer(Clock.live).orDie

        override def send(msg: Message.Direct[A]): IO[zio.keeper.Error, Message[A]] =
          protocol.onMessage(msg)
      }
    }
}

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
import zio.keeper.membership.swim.protocols.FailureDetection.{ Ack, Ping, PingReq }
import zio.keeper.membership.swim.{ ConversationId, Message, Nodes, Protocol }
import zio.logging.{ Logging, log }
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.environment.TestClock
import zio.test.{ assert, _ }

object FailureDetectionSpec extends DefaultRunnableSpec {

  val logger     = Logging.console((_, line) => line)
  val nodesLayer = (ZLayer.requires[Clock] ++ logger) >>> Nodes.live

  val recorder: ZLayer[Clock with Console, Nothing, ProtocolRecorder[FailureDetection]] =
    (ZLayer.requires[Clock] ++ nodesLayer ++ logger ++ ConversationId.live) >>>
      ProtocolRecorder
        .make(
          FailureDetection
            .protocol(1.second, 500.milliseconds)
            .flatMap(_.debug)
        )
        .orDie

  val nodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
  val nodeAddress2 = NodeAddress(Array(11, 22, 33, 44), 1111)
  val nodeAddress3 = NodeAddress(Array(2, 3, 4, 5), 1111)

  val spec = suite("failure detection")(
    testM("Ping healthy Nodes periodically") {
      for {
        recorder <- ProtocolRecorder[FailureDetection]
        _        <- addNode(nodeAddress1)
        _        <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _        <- addNode(nodeAddress2)
        _        <- changeNodeState(nodeAddress2, NodeState.Healthy)
        _        <- addNode(nodeAddress3)
        _        <- changeNodeState(nodeAddress3, NodeState.Unreachable)
        _         <- TestClock.adjust(10.milliseconds)
        addr1 <- ZIO.foreach(1 to 10) { _ =>
                  recorder
                    .collectFirstM {
                      case Message.Direct(nodeAddr, ackId, Ping) =>
                        recorder.send(Message.Direct(nodeAddr, ackId, Ack)).as(nodeAddr)
                    }
                    .tap(_ => TestClock.adjust(1.second))
                }
      } yield assert(addr1.toSet)(equalTo(Set(nodeAddress2, nodeAddress1)))
    },
    testM("should change to Dead if there is no nodes to send PingReq") {
      for {
        recorder  <- ProtocolRecorder[FailureDetection]
        _         <- addNode(nodeAddress1)
        _         <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _         <- TestClock.adjust(10.milliseconds)
        messages  <- recorder.collect(1)
        _         <- TestClock.adjust(750.milliseconds)
        _         <- recorder.collect(1)
        nodeState <- nodeState(nodeAddress1)
      } yield assert(messages)(equalTo(List(Message.Direct(nodeAddress1, 1, Ping)))) &&
        assert(nodeState)(equalTo(NodeState.Dead))
    },
    testM("should send PingReq to other node") {
      for {
        recorder <- ProtocolRecorder[FailureDetection]
        _        <- addNode(nodeAddress1)
        _        <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _        <- addNode(nodeAddress2)
        _        <- changeNodeState(nodeAddress2, NodeState.Healthy)
        _        <- TestClock.adjust(10.milliseconds)
        _ <- recorder.collectFirstM {
              case Message.Direct(`nodeAddress2`, ackId, Ping) =>
                recorder.send(Message.Direct(nodeAddress2, ackId, Ack))
            }
        _ <- TestClock.adjust(1.second)
        _ <- recorder.collectFirstM {
              case Message.Direct(`nodeAddress1`, _, Ping) =>
                ZIO.unit
            }
        _ <- TestClock.adjust(1.second)
        msg <- recorder.collectFirstM {
                case Message.Direct(`nodeAddress2`, _, msg: PingReq) =>
                  ZIO.succeed(msg)
              }
        nodeState <- nodeState(nodeAddress1)
      } yield assert(msg)(equalTo(PingReq(nodeAddress1))) && assert(nodeState)(equalTo(NodeState.Unreachable))
    }
  ).provideCustomLayer(ConversationId.live ++ logger ++ nodesLayer ++ recorder)

}

object ProtocolRecorder {
  type ProtocolRecorder[A] = Has[ProtocolRecorder.Service[A]]

  trait Service[A] {
    def collectFirstM[R, E, B](pf: PartialFunction[Message[A], ZIO[R, E, B]]): ZIO[R, E, B]
    def collectM[R, E, B](n: Long)(pf: PartialFunction[Message[A], ZIO[R, E, B]]): ZIO[R, E, List[B]]
    def collect(n: Long): UIO[List[Message[A]]]
    def send(msg: Message.Direct[A]): IO[zio.keeper.Error, Message[A]]
  }

  def apply[A: Tag] = ZIO.environment[ProtocolRecorder[A]].map(_.get)

  def make[R, E, A: Tag](
    protocolFactory: ZIO[R, E, Protocol[A]]
  ): ZLayer[Clock with Logging with Nodes with R, E, ProtocolRecorder[A]] =
    ZLayer.fromEffect {
      def loop(
        messageQueue: zio.Queue[Message[A]],
        message: Message[A]
      ): ZIO[Clock with Logging with Nodes, Nothing, Unit] =
        (message match {
          case Message.WithTimeout(message, action, timeout) =>
            messageQueue.offer(message).unit *> action.delay(timeout).flatMap(loop(messageQueue, _)).fork.unit
          case msg =>
            messageQueue.offer(msg).unit
        }) *> log.info("recorded message: " + message)
      for {
        protocol     <- protocolFactory
        messageQueue <- ZQueue.bounded[Message[A]](100)
        _            <- protocol.produceMessages.foreach(loop(messageQueue, _)).fork

        stream = ZStream.fromQueue(messageQueue)

      } yield new Service[A] {

        override def collectFirstM[R1, E1, B](pf: PartialFunction[Message[A], ZIO[R1, E1, B]]): ZIO[R1, E1, B] =
          stream.collectM(pf).runHead.map(_.get)

        override def collectM[R1, E1, B](
          n: Long
        )(pf: PartialFunction[Message[A], ZIO[R1, E1, B]]): ZIO[R1, E1, List[B]] =
          stream.collectM(pf).take(n).runCollect

        override def collect(n: Long): UIO[List[Message[A]]] =
          stream.take(n).runCollect

        override def send(msg: Message.Direct[A]): IO[zio.keeper.Error, Message[A]] =
          protocol.onMessage(msg)
      }
    }
}

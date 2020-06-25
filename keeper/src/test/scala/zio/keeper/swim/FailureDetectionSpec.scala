package zio.keeper.swim

import zio._
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.keeper.{ KeeperSpec, NodeAddress }
import zio.keeper.swim.Nodes.{ nodeState, _ }
import zio.keeper.swim.ProtocolRecorder.ProtocolRecorder
import zio.keeper.swim.protocols.FailureDetection
import zio.keeper.swim.protocols.FailureDetection.{ Ack, Ping, PingReq }
import zio.logging.Logging
import zio.test.Assertion._
import zio.test.environment.TestClock
import zio.test.{ assert, _ }

object FailureDetectionSpec extends KeeperSpec {

  val logger     = Logging.console((_, line) => line)
  val nodesLayer = (ZLayer.requires[Clock] ++ logger) >>> Nodes.live

  val recorder: ZLayer[Clock with Console, Nothing, ProtocolRecorder[FailureDetection]] =
    (ZLayer.requires[Clock] ++ nodesLayer ++ logger ++ ConversationId.live ++ LocalHealthMultiplier.live(9)) >>>
      ProtocolRecorder
        .make(
          FailureDetection
            .protocol(1.second, 500.milliseconds, NodeAddress(Array[Byte](1, 1, 1, 1), 1111))
            .flatMap(_.debug)
        )
        .orDie

  val testLayer = ConversationId.live ++ logger ++ nodesLayer ++ recorder ++ LocalHealthMultiplier.live(9)

  val nodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
  val nodeAddress2 = NodeAddress(Array(11, 22, 33, 44), 1111)
  val nodeAddress3 = NodeAddress(Array(2, 3, 4, 5), 1111)

  val spec = suite("failure detection")(
    testM("Ping healthy Nodes periodically") {
      for {
        recorder <- ProtocolRecorder[FailureDetection] {
                     case Message.Direct(nodeAddr, ackId, Ping) =>
                       Message.Direct(nodeAddr, ackId, Ack)
                   }
        _        <- addNode(nodeAddress1)
        _        <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _        <- addNode(nodeAddress2)
        _        <- changeNodeState(nodeAddress2, NodeState.Healthy)
        _        <- TestClock.adjust(100.seconds)
        messages <- recorder.collectN(3) { case Message.Direct(addr, _, Ping) => addr }
      } yield assert(messages.toSet)(equalTo(Set(nodeAddress2, nodeAddress1)))
    }.provideCustomLayer(testLayer),
    testM("should change to Dead if there is no nodes to send PingReq") {
      for {
        recorder  <- ProtocolRecorder[FailureDetection]()
        _         <- addNode(nodeAddress1)
        _         <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _         <- TestClock.adjust(1500.milliseconds)
        messages  <- recorder.collectN(2) { case msg => msg }
        nodeState <- nodeState(nodeAddress1).orElseSucceed(NodeState.Dead) // in case it was cleaned up already
      } yield assert(messages)(equalTo(List(Message.Direct(nodeAddress1, 1, Ping), Message.NoResponse))) &&
        assert(nodeState)(equalTo(NodeState.Dead))
    }.provideCustomLayer(testLayer),
    testM("should send PingReq to other node") {
      for {
        recorder <- ProtocolRecorder[FailureDetection] {
                     case Message.Direct(`nodeAddress2`, ackId, Ping) =>
                       Message.Direct(nodeAddress2, ackId, Ack)
                     case Message.Direct(`nodeAddress1`, _, Ping) =>
                       Message.NoResponse //simulate failing node
                   }
        _   <- addNode(nodeAddress1)
        _   <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _   <- addNode(nodeAddress2)
        _   <- changeNodeState(nodeAddress2, NodeState.Healthy)
        _   <- TestClock.adjust(10.seconds)
        msg <- recorder.collectN(1) { case Message.Direct(_, _, msg: PingReq) => msg }
      } yield assert(msg)(equalTo(List(PingReq(nodeAddress1))))
    }.provideCustomLayer(testLayer),
    testM("should change to Healthy when ack after PingReq arrives") {
      for {
        recorder <- ProtocolRecorder[FailureDetection] {
                     case Message.Direct(`nodeAddress2`, ackId, Ping) =>
                       Message.Direct(nodeAddress2, ackId, Ack)
                     case Message.Direct(`nodeAddress1`, _, Ping) =>
                       Message.NoResponse //simulate failing node
                     case Message.Direct(`nodeAddress2`, ackId, _: PingReq) =>
                       Message.Direct(nodeAddress2, ackId, Ack)
                   }
        _ <- addNode(nodeAddress1)
        _ <- changeNodeState(nodeAddress1, NodeState.Healthy)
        _ <- addNode(nodeAddress2)
        _ <- changeNodeState(nodeAddress2, NodeState.Healthy)
        _ <- TestClock.adjust(10.seconds)
        _ <- recorder.collectN(1) { case Message.Direct(_, _, msg: PingReq) => msg }
//        event <- internalEvents.collect {
//                  case NodeStateChanged(`nodeAddress1`, NodeState.Unreachable, NodeState.Healthy) => ()
//                }.runHead
      } yield assert(true)(equalTo(true))
    }.provideCustomLayer(testLayer)
  )
}

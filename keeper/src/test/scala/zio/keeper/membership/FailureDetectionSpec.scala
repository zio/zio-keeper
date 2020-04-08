package zio.keeper.membership

import zio.duration._
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.protocols.FailureDetection
import zio.keeper.membership.swim.protocols.FailureDetection.Ping
import zio.keeper.membership.swim.{Message, Nodes}
import zio.logging.Logging
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestClock


object FailureDetectionSpec extends DefaultRunnableSpec{

  val logger = Logging.console((_, line) => line)

  val spec = suite("failure detection")(
    testM("Ping") {
      val nodeAddress1 = NodeAddress(Array(1,2,3,4), 1111)
      val nodeAddress2 = NodeAddress(Array(11,22,33,44), 1111)
      Nodes.make.flatMap(nodes =>
        for {
          protocol <- FailureDetection.protocol(nodes, 1.second)
          _ <- nodes.addNode(nodeAddress1)
          _ <- nodes.changeNodeState(nodeAddress1, NodeState.Healthy)
          _ <- nodes.addNode(nodeAddress2)
          _ <- nodes.changeNodeState(nodeAddress2, NodeState.Healthy)
          _ <- TestClock.adjust(1.second)
          ping <- protocol.produceMessages.take(2).runCollect
        } yield assert(ping)(equalTo(List(Message.Direct(nodeAddress2, Ping(1)), Message.Direct(nodeAddress1, Ping(2)))))
      )
    }
  ).provideCustomLayer(logger)

}

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
      val testNodeAddress = NodeAddress(Array(1,2,3,4), 1111)
      Nodes.make.flatMap(nodes =>
        for {
          protocol <- FailureDetection.protocol(nodes, 1.second)
          _ <- nodes.addNode(testNodeAddress)
          _ <- nodes.changeNodeState(testNodeAddress, NodeState.Healthy)
          _ <- TestClock.adjust(1.second)
          ping <- protocol.produceMessages.take(3).runCollect
        } yield assert(ping)(equalTo(List(Message.Direct(testNodeAddress, Ping(1)))))
      )
    }
  ).provideCustomLayer(logger)

}

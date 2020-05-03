package zio.keeper.membership

import zio.keeper.NodeAddress
import zio.keeper.membership.swim.Nodes
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.Nodes.NodeState.{ Death, Healthy, Suspicion }
import zio.logging.Logging
import zio.stream.Sink
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

object NodesSpec extends DefaultRunnableSpec {

  val logger = Logging.console((_, line) => line)

  val spec = suite("nodes")(
    testM("add node") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)
      Nodes.make.flatMap(
        nodes =>
          for {
            next0 <- nodes.next
            _     <- nodes.addNode(testNodeAddress)
            next1 <- nodes.next
            _     <- nodes.changeNodeState(testNodeAddress, NodeState.Healthy)
            next2 <- nodes.next
          } yield assert(next0)(isNone) && assert(next1)(isNone) && assert(next2)(isSome(equalTo(testNodeAddress)))
      )
    },
    testM("add node twice") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)
      Nodes.make.flatMap(
        nodes =>
          for {
            _    <- nodes.addNode(testNodeAddress)
            _    <- nodes.changeNodeState(testNodeAddress, NodeState.Healthy)
            _    <- nodes.addNode(testNodeAddress)
            next <- nodes.next
          } yield assert(next)(isSome(equalTo(testNodeAddress)))
      )
    },
    testM("should propagate events") {
      val testNodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
      val testNodeAddress2 = NodeAddress(Array(1, 2, 3, 4), 1112)
      Nodes.make.flatMap(
        nodes =>
          for {
            _       <- nodes.addNode(testNodeAddress1)
            _       <- nodes.changeNodeState(testNodeAddress1, NodeState.Healthy)
            _       <- nodes.changeNodeState(testNodeAddress1, NodeState.Suspicion)
            _       <- nodes.changeNodeState(testNodeAddress1, NodeState.Death)
            events1 <- nodes.events.run(Sink.collectAllN[MembershipEvent](3))
            _       <- nodes.addNode(testNodeAddress2)
            _       <- nodes.changeNodeState(testNodeAddress2, NodeState.Healthy)
            _       <- nodes.changeNodeState(testNodeAddress2, NodeState.Suspicion)
            _       <- nodes.changeNodeState(testNodeAddress2, NodeState.Death)
            events2 <- nodes.events.run(Sink.collectAllN[MembershipEvent](3))
          } yield assert(events1)(
            hasSameElements(
              List(
                MembershipEvent.Join(testNodeAddress1),
                MembershipEvent.NodeStateChanged(testNodeAddress1, Healthy, Suspicion),
                MembershipEvent.NodeStateChanged(testNodeAddress1, Suspicion, Death)
              )
            )
          ) && assert(events2)(
            hasSameElements(
              List(
                MembershipEvent.Join(testNodeAddress2),
                MembershipEvent.NodeStateChanged(testNodeAddress2, Healthy, Suspicion),
                MembershipEvent.NodeStateChanged(testNodeAddress2, Suspicion, Death)
              )
            )
          )
      )
    }
  ).provideCustomLayer(logger)

}

package zio.keeper.swim

import zio.{ ZIO, ZLayer }
import zio.clock.Clock
import zio.keeper.MembershipEvent
import zio.keeper.swim.Nodes._
import zio.keeper.{ KeeperSpec, NodeAddress }
import zio.logging.Logging
import zio.test.Assertion._
import zio.test._

object NodesSpec extends KeeperSpec {

  val logger = Logging.console((_, line) => line)

  val spec = suite("nodes")(
    testM("add node") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)
      for {
        next0 <- nextNode()
        _     <- addNode(testNodeAddress)
        next1 <- nextNode()
        _     <- changeNodeState(testNodeAddress, NodeState.Healthy)
        next2 <- nextNode()
      } yield assert(next0)(isNone) && assert(next1)(isNone) && assert(next2)(
        isSome(equalTo((testNodeAddress, NodeState.Healthy)))
      )
    },
    testM("add node twice") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)
      for {
        _    <- addNode(testNodeAddress)
        _    <- changeNodeState(testNodeAddress, NodeState.Healthy)
        _    <- addNode(testNodeAddress)
        next <- nextNode()
      } yield assert(next)(isSome(equalTo((testNodeAddress, NodeState.Healthy))))
    },
    testM("exclude node") {
      val testNodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
      val testNodeAddress2 = NodeAddress(Array(1, 2, 3, 4), 1112)
      for {
        _    <- addNode(testNodeAddress1)
        _    <- changeNodeState(testNodeAddress1, NodeState.Healthy)
        _    <- addNode(testNodeAddress2)
        _    <- changeNodeState(testNodeAddress2, NodeState.Healthy)
        next <- ZIO.foreach(1 to 10)(_ => nextNode(Some(testNodeAddress2)))
      } yield assert(next.flatten.toSet)(equalTo(Set((testNodeAddress1, NodeState.Healthy: NodeState))))
    },
    testM("should propagate events") {
      val testNodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
      val testNodeAddress2 = NodeAddress(Array(1, 2, 3, 4), 1112)
      for {
        _       <- addNode(testNodeAddress1)
        _       <- changeNodeState(testNodeAddress1, NodeState.Healthy)
        _       <- changeNodeState(testNodeAddress1, NodeState.Suspicion)
        _       <- changeNodeState(testNodeAddress1, NodeState.Dead)
        events1 <- Nodes.events.take(2).runCollect
        _       <- addNode(testNodeAddress2)
        _       <- changeNodeState(testNodeAddress2, NodeState.Healthy)
        _       <- changeNodeState(testNodeAddress2, NodeState.Suspicion)
        _       <- changeNodeState(testNodeAddress2, NodeState.Dead)
        events2 <- Nodes.events.take(2).runCollect
      } yield assert(events1)(
        hasSameElements(
          List(
            MembershipEvent.Join(testNodeAddress1),
            MembershipEvent.Leave(testNodeAddress1)
          )
        )
      ) && assert(events2)(
        hasSameElements(
          List(
            MembershipEvent.Join(testNodeAddress2),
            MembershipEvent.Leave(testNodeAddress2)
          )
        )
      )
    }
  ).provideCustomLayer((ZLayer.requires[Clock] ++ logger) >>> Nodes.live)

}

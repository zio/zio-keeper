package zio.keeper.membership

import zio.keeper.membership.swim.Nodes
import zio.keeper.membership.swim.Nodes.NodeState
import zio.test.Assertion._
import zio.test.{DefaultRunnableSpec, _}

object NodesSpec extends DefaultRunnableSpec {

  val spec = suite("nodes")(
    testM("add node") {
      val testNodeAddress = NodeAddress(Array(1,2,3,4), 1111)
      Nodes.make.flatMap(nodes =>
        for {
          next0 <- nodes.next
          _ <- nodes.addNode(testNodeAddress)
          next1 <- nodes.next
          _ <- nodes.changeNodeState(testNodeAddress, NodeState.Healthy)
          next2 <- nodes.next
        } yield assert(next0)(isNone) && assert(next1)(isNone) && assert(next2)(isSome(equalTo(testNodeAddress)))
      )
    },
    testM("add node twice") {
      val testNodeAddress = NodeAddress(Array(1,2,3,4), 1111)
      Nodes.make.flatMap(nodes =>
        for {
          _ <- nodes.addNode(testNodeAddress)
          _ <- nodes.changeNodeState(testNodeAddress, NodeState.Healthy)
          _ <- nodes.addNode(testNodeAddress)
          next <- nodes.next
        } yield assert(next)(isSome(equalTo(testNodeAddress)))
      )
    }


  )

}

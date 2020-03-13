package zio.keeper.membership

import zio.keeper.membership.GossipState.StateDiff
import zio.test.Assertion.equalTo
import zio.test.{ DefaultRunnableSpec, assert, suite, test }

import scala.collection.immutable.SortedSet

object GossipStateSpec extends DefaultRunnableSpec {

  def spec = suite("GossipStateSpec")(
    test("should find different") {
      val socketAddr = NodeAddress(Array(127, 0, 0, 1), 11111)
      val member1    = Member(NodeId.generateNew, socketAddr)
      val member2    = Member(NodeId.generateNew, socketAddr)
      val member3    = Member(NodeId.generateNew, socketAddr)
      val local      = GossipState(SortedSet(member1, member3))
      val remote     = GossipState(SortedSet(member2, member3))
      val diff       = local.diff(remote)

      assert(diff)(equalTo(StateDiff(SortedSet(member1), SortedSet(member2))))
    }
  )
}

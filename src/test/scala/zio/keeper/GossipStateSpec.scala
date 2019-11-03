package zio.keeper

import zio.keeper.GossipState.StateDiff
import zio.nio.{ InetAddress, SocketAddress }
import zio.test.DefaultRunnableSpec
import zio.test._
import zio.test.Assertion._
import scala.collection.SortedSet

object GossipStateSpec
    extends DefaultRunnableSpec(
      suite("GossipStateSpec")(
        testM("should find different") {
          for {
            inetAddr   <- InetAddress.byAddress(Array(127, 0, 0, 1)).orDie
            socketAddr <- SocketAddress.inetSocketAddress(inetAddr, 9090).orDie
          } yield {
            val member1 = Member(NodeId.generateNew, socketAddr)
            val member2 = Member(NodeId.generateNew, socketAddr)
            val member3 = Member(NodeId.generateNew, socketAddr)
            val local   = GossipState(SortedSet(member1, member3))
            val remote  = GossipState(SortedSet(member2, member3))
            val diff    = local.diff(remote)

            assert(diff, equalTo(StateDiff(SortedSet(member1), SortedSet(member2))))
          }

        }
      )
    )

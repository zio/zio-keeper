package zio.keeper

import zio.Chunk
import zio.duration._
import zio.test.{ DefaultRunnableSpec, TestAspect }

abstract class KeeperSpec extends DefaultRunnableSpec {
  override val aspects = List(TestAspect.timeout(180.seconds))

  def address(n: Int): NodeAddress =
    NodeAddress(Chunk.empty, n)
}

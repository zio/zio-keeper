package zio.keeper

import zio.duration._
import zio.test.{ DefaultRunnableSpec, TestAspect }

abstract class KeeperSpec extends DefaultRunnableSpec {
  override val aspects = List(TestAspect.timeout(180.seconds))

  def address(n: Int): NodeAddress =
    NodeAddress(Array.emptyByteArray, n)
}

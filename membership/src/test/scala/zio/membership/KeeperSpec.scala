package zio.membership

import zio.duration._
import zio.test.{ DefaultRunnableSpec, TestAspect }

abstract class KeeperSpec extends DefaultRunnableSpec {
  override val aspects = List(TestAspect.timeout(60.seconds))
}

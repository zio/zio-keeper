package zio.membership

import zio._
import java.{ util => ju }

object uuid {

  val makeRandom: UIO[ju.UUID] = ZIO.effectTotal(ju.UUID.randomUUID())

}

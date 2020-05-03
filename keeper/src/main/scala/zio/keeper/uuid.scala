package zio.keeper

import java.util.UUID

import zio.{ UIO, ZIO }

object uuid {

  val makeRandomUUID: UIO[UUID] = ZIO.effectTotal(UUID.randomUUID())

}

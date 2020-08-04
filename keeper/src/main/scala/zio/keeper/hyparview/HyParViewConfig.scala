package zio.keeper.hyparview

import zio.{ UIO, ULayer, ZLayer }

object HyParViewConfig {

  final private[hyparview] case class Config(
    activeViewCapacity: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int,
    shuffleNActive: Int,
    shuffleNPassive: Int,
    shuffleTTL: Int,
    connectionBuffer: Int,
    userMessagesBuffer: Int,
    concurrentIncomingConnections: Int
  ) {

    val prettyPrint: String =
      s"""activeViewCapacity: $activeViewCapacity
         |passiveViewCapacity: $passiveViewCapacity
         |arwl: $arwl
         |prwl: $prwl
         |shuffleNActive: $shuffleNActive
         |shuffleNPassive: $shuffleNPassive
         |shuffleTTL: $shuffleTTL
         |connectionBuffer: $connectionBuffer
         |userMessagesBuffer: $userMessagesBuffer
         |concurrentIncomingConnections: $concurrentIncomingConnections""".stripMargin
  }

  def staticConfig(
    activeViewCapacity: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int,
    shuffleNActive: Int,
    shuffleNPassive: Int,
    shuffleTTL: Int,
    connectionBuffer: Int,
    userMessagesBuffer: Int,
    concurrentIncomingConnections: Int
  ): ULayer[HyParViewConfig] =
    ZLayer.succeed {
      new Service {
        val getConfig: UIO[Config] =
          UIO.succeed {
            Config(
              activeViewCapacity,
              passiveViewCapacity,
              arwl,
              prwl,
              shuffleNActive,
              shuffleNPassive,
              shuffleTTL,
              connectionBuffer,
              userMessagesBuffer,
              concurrentIncomingConnections
            )
          }
      }
    }

  trait Service {
    val getConfig: UIO[Config]
  }
}

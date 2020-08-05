package zio.keeper.hyparview

import zio.{ UIO, ULayer, URIO, ZIO, ZLayer }
import zio.stm.{ STM, ZSTM }

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

  trait Service {
    val getConfigSTM: STM[Nothing, Config]

    val getConfig: UIO[Config] =
      getConfigSTM.commit
  }

  val getConfig: URIO[HyParViewConfig, Config] =
    ZIO.accessM(_.get.getConfig)

  val getConfigSTM: ZSTM[HyParViewConfig, Nothing, Config] =
    ZSTM.accessM(_.get.getConfigSTM)

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
        val getConfigSTM: STM[Nothing, Config] =
          STM.succeed {
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
}

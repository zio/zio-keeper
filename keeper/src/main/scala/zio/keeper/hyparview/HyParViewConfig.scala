package zio.keeper.hyparview

import zio.{ UIO, ULayer, URIO, ZIO, ZLayer }
import zio.keeper.NodeAddress
import zio.stm._

object HyParViewConfig {

  final private[hyparview] case class Config(
    address: NodeAddress,
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
      s"""address: $address
         |activeViewCapacity: $activeViewCapacity
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
    val getConfigSTM: USTM[Config]

    val getConfig: UIO[Config] =
      getConfigSTM.commit
  }

  val getConfig: URIO[HyParViewConfig, Config] =
    ZIO.accessM(_.get.getConfig)

  val getConfigSTM: ZSTM[HyParViewConfig, Nothing, Config] =
    ZSTM.accessM(_.get.getConfigSTM)

  def staticConfig(
    address: NodeAddress,
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
        val getConfigSTM: USTM[Config] =
          STM.succeed {
            Config(
              address,
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

package zio.keeper.hyparview

import zio.{ ULayer, URIO, ZIO, ZLayer }
import zio.keeper.NodeAddress
import zio.stm._
import java.time.Duration

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
    concurrentIncomingConnections: Int,
    viewsEventBuffer: Int,
    neighborBackoff: Duration
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
         |concurrentIncomingConnections: $concurrentIncomingConnections
         |viewsEventBuffer: $viewsEventBuffer
         |neighborBackoff: $neighborBackoff""".stripMargin
  }

  trait Service {
    val getConfigSTM: USTM[Config]
  }

  val getConfig: URIO[HyParViewConfig, Config] =
    ZIO.accessM(_.get.getConfigSTM.commit)

  val getConfigSTM: ZSTM[HyParViewConfig, Nothing, Config] =
    ZSTM.accessM(_.get.getConfigSTM)

  def static(
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
    concurrentIncomingConnections: Int,
    viewsEventBuffer: Int = 256,
    neighborBackoff: Duration = Duration.ofSeconds(10)
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
              concurrentIncomingConnections,
              viewsEventBuffer,
              neighborBackoff
            )
          }
      }
    }
}

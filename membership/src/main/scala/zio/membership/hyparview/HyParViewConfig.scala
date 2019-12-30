package zio.membership.hyparview

import zio._
import zio.macros.delegate._

trait HyParViewConfig {
  val hyParViewConfig: HyParViewConfig.Service[Any]
}

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
    nWorkers: Int
  )

  def withStaticConfig(
    activeViewCapacity: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int,
    shuffleNActive: Int,
    shuffleNPassive: Int,
    shuffleTTL: Int,
    connectionBuffer: Int,
    userMessagesBuffer: Int,
    nWorkers: Int
  ) =
    enrichWith[HyParViewConfig](
      HyParViewConfig(
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
          nWorkers
        )
      )
    )

  def apply(config: Config): HyParViewConfig =
    new HyParViewConfig {

      val hyParViewConfig = new Service[Any] {
        val getConfig = ZIO.succeed(config)
      }
    }

  trait Service[-R] {
    val getConfig: URIO[R, Config]
  }
}

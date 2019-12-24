package zio.membership.hyparview

import zio._
import zio.macros.delegate._

final private[hyparview] case class Config(
  activeViewCapacity: Int,
  passiveViewCapacity: Int,
  arwl: Int,
  prwl: Int,
  shuffleNActive: Int,
  shuffleNPassive: Int,
  shuffleTTL: Int
)

trait Cfg {
  val cfg: Cfg.Service[Any]
}

object Cfg {

  def withStaticConfig(
    config: Config
  ) = enrichWith[Cfg](
    Cfg(config)
  )

  def apply(config: Config): Cfg =
    new Cfg {

      val cfg = new Service[Any] {
        val getConfig = ZIO.succeed(config)
      }
    }

  trait Service[-R] {
    val getConfig: URIO[R, Config]
  }

}

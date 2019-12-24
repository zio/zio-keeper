package zio.membership

import zio._

package object hyparview extends Cfg.Service[Cfg] {

  val getConfig =
    ZIO.accessM[Cfg](_.cfg.getConfig)

}

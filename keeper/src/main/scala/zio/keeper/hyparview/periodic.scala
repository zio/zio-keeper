package zio.keeper.hyparview

import zio.ZIO
import zio.logging.Logging
import zio.logging.log
import zio.stm.ZSTM
import zio.keeper.hyparview.Message.Shuffle

object periodic {

  def doShuffle: ZIO[Views with Logging with HyParViewConfig with TRandom, Nothing, ViewState] =
    HyParViewConfig.getConfig.flatMap { config =>
      Views.activeView
        .map(_.toList)
        .flatMap { nodes =>
          TRandom.selectOne(nodes).flatMap {
            case None => Views.viewState
            case Some(node) =>
              for {
                active    <- TRandom.selectN(nodes.filter(_ != node), config.shuffleNActive)
                passive   <- Views.passiveView.flatMap(p => TRandom.selectN(p.toList, config.shuffleNPassive))
                state     <- Views.viewState
                localAddr <- Views.myself
                _ <- Views.send(
                      node,
                      Shuffle(localAddr, localAddr, active.toSet, passive.toSet, TimeToLive(config.shuffleTTL))
                    )
              } yield state
          }
        }
        .commit
    }

  def doReport: ZIO[Views with Logging, Nothing, Unit] =
    ZSTM.atomically {
      for {
        active      <- Views.activeViewSize
        activeCapa  <- Views.activeViewCapacity
        passive     <- Views.passiveViewSize
        passiveCapa <- Views.passiveViewCapacity
        localAddr   <- Views.myself
      } yield log.info(
        s"HyParView: { addr: $localAddr, activeView: $active/$activeCapa, passiveView: $passive/$passiveCapa }"
      )
    }.flatten
}

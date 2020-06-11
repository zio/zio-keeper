package zio.keeper.hyparview

import zio.ZIO
import zio.logging.Logging
import zio.logging.log
import zio.stm.{ STM, ZSTM }

object periodic {

  def doShuffle: ZIO[Views with Logging with HyParViewConfig with TRandom, Nothing, ViewState] =
    getConfig.flatMap { config =>
      Views.activeView
        .map(_.toList)
        .flatMap { nodes =>
          TRandom.selectOne(nodes).flatMap {
            case None => STM.succeed(Views.viewState.commit)
            case Some(node) =>
              for {
                active    <- TRandom.selectN(nodes.filter(_ != node), config.shuffleNActive)
                passive   <- Views.passiveView.flatMap(p => TRandom.selectN(p.toList, config.shuffleNPassive))
                state     <- Views.viewState
                localAddr <- Views.myself
              } yield Views
                .send(
                  node,
                  ActiveProtocol
                    .Shuffle(localAddr, localAddr, active, passive, TimeToLive(config.shuffleTTL))
                )
                .as(state)
          }
        }
        .commit
        .flatten
        .eventually
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

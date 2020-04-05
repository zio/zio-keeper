package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.SendError
import zio.logging.log
import zio.logging.Logging.Logging

object periodic {

  def doShuffle[T: Tagged]: ZIO[Views[T] with Logging with HyParViewConfig with TRandom, Nothing, ViewState] =
    getConfig.flatMap { config =>
      val go: ZIO[TRandom with Views[T], SendError, ViewState] =
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
      go.eventually
    }

  def doReport[T: Tagged]: ZIO[Views[T] with Logging, Nothing, Unit] =
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

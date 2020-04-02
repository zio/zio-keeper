package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.SendError
import zio.logging.log
import zio.logging.Logging.Logging

object periodic {

  def doShuffle[T: Tagged]: ZIO[Views[T] with Logging with HyParViewConfig with TRandom, Nothing, ViewState] =
    Views.using[T].apply { views =>
      TRandom.using { tRandom =>
        getConfig.flatMap { config =>
          val go: IO[SendError, ViewState] =
            views.activeView
              .map(_.toList)
              .flatMap { nodes =>
                tRandom.selectOne(nodes).flatMap {
                  case None => STM.succeed(views.viewState.commit)
                  case Some(node) =>
                    for {
                      active  <- tRandom.selectN(nodes.filter(_ != node), config.shuffleNActive)
                      passive <- views.passiveView.flatMap(p => tRandom.selectN(p.toList, config.shuffleNPassive))
                      state   <- views.viewState
                    } yield views
                      .send(
                        node,
                        ActiveProtocol
                          .Shuffle(views.myself, views.myself, active, passive, TimeToLive(config.shuffleTTL))
                      )
                      .as(state)
                }
              }
              .commit
              .flatten
          go.eventually
        }
      }
    }

  def doReport[T: Tagged]: ZIO[Views[T] with Logging, Nothing, Unit] =
    Views
      .using[T]
      .apply { views =>
        STM.atomically {
          for {
            active  <- views.activeViewSize
            passive <- views.passiveViewSize
          } yield log.info(
            s"HyParView: { addr: ${views.myself}, activeView: $active/${views.activeViewCapacity}, passiveView: $passive/${views.passiveViewCapacity} }"
          )
        }
      }
      .flatten
}

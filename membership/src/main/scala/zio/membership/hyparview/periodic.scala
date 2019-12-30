package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.{ SendError, log }
import zio.logging.Logging

object periodic {

  def doShuffle[T]: ZIO[Views[T] with Logging[String] with HyParViewConfig, Nothing, ViewState] =
    Views.using[T] { views =>
      getConfig.flatMap { config =>
        val go: IO[SendError, ViewState] =
          views.activeView
            .map(_.toList)
            .flatMap { nodes =>
              views.selectOne(nodes).flatMap {
                case None => STM.succeed(views.viewState.commit)
                case Some(node) =>
                  for {
                    active  <- views.selectN(nodes.filter(_ != node), config.shuffleNActive)
                    passive <- views.passiveView.flatMap(p => views.selectN(p.toList, config.shuffleNPassive))
                    state   <- views.viewState
                  } yield views
                    .send(
                      node,
                      ActiveProtocol.Shuffle(views.myself, views.myself, active, passive, TimeToLive(config.shuffleTTL))
                    )
                    .as(state)
              }
            }
            .commit
            .flatten
        go.eventually
      }
    }

  def doReport[T]: ZIO[Views[T] with Logging[String], Nothing, Unit] =
    Views
      .using[T] { views =>
        STM.atomically {
          for {
            active  <- views.activeView
            passive <- views.passiveView
          } yield log.info(
            s"HyParView: { addr: ${views.myself}, activeView: $active/${views.activeViewCapacity}, passiveView: $passive/${views.passiveViewCapacity} }"
          )
        }
      }
      .flatten

}

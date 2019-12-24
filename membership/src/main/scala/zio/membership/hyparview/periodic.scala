package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.log
import zio.logging.Logging

/**
 * periodic tasks that are part of hyparview protocol.
 */
private[hyparview] object periodic {

  def doNeighbor[E, T](
    sendInitial: (T, InitialProtocol[T]) => IO[E, Unit]
  ): ZIO[Views[T], E, Int] =
    Views.using[T] { views =>
      STM
        .atomically {
          for {
            full <- views.isActiveViewFull
            promoted <- if (full) STM.succeed(None)
                       else views.passiveView.flatMap(a => views.selectOne(a.toList))
            active <- views.activeViewSize
          } yield (promoted, active)
        }
        .flatMap {
          case (Some(node), active) =>
            sendInitial(node, InitialProtocol.Neighbor(views.myself, active <= 0)).as(active)
          case (_, active) =>
            ZIO.succeed(active)
        }
    }

  def doShuffle[T]: ZIO[Views[T] with Logging[String] with Cfg, Nothing, Int] =
    Views.using[T] { views =>
      getConfig.flatMap { config =>
        def go(nodes: List[T]): UIO[Unit] =
          views.selectOne(nodes.toList).commit.flatMap {
            case None => ZIO.unit
            case Some(node) =>
              (for {
                active  <- views.selectN(nodes.filter(_ != node), config.shuffleNActive)
                passive <- views.passiveView.flatMap(p => views.selectN(p.toList, config.shuffleNPassive))
              } yield views.send(
                node,
                ActiveProtocol.Shuffle(views.myself, views.myself, active, passive, TimeToLive(config.shuffleTTL))
              )).commit.flatten.orElse(go(nodes.filterNot(_ == node)))
          }
        views.activeView.commit.flatMap(a => go(a.toList)) *> views.activeViewSize.commit
      }
    }

  private[hyparview] def doReport[T]: ZIO[Views[T] with Logging[String], Nothing, Unit] =
    Views
      .using[T] { views =>
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

package zio.membership.hyparview

import zio._
import zio.membership.transport.Transport
import zio.stm._
import zio.console.Console

/**
 * periodic tasks that are part of hyparview protocol.
 */
private[hyparview] object periodic {

  def doNeighbor[T](
    sendInitial: (T, InitialProtocol[T]) => UIO[Unit]
  ): ZIO[Env[T], Nothing, Int] =
    Env.using[T] { env =>
      STM
        .atomically {
          for {
            promoted <- env.promoteRandom
            active   <- env.activeView.keys.map(_.size)
          } yield (promoted, active)
        }
        .flatMap {
          case (Some(node), active) => sendInitial(node, InitialProtocol.Neighbor(env.myself, active <= 0)).as(active)
          case (_, active)          => ZIO.succeed(active)
        }
    }

  def doShuffle[T](
    implicit ev: Tagged[ActiveProtocol[T]]
  ): ZIO[Console with Env[T] with Transport[T], Nothing, Int] =
    Env.using[T] { env =>
      (for {
        nodes  <- env.activeView.keys
        target <- env.selectOne(nodes)
        task <- target match {
                 case None => STM.succeed(ZIO.unit)
                 case Some(node) =>
                   for {
                     active  <- env.selectN(nodes.filter(_ != node), env.cfg.shuffleNActive)
                     passive <- env.passiveView.toList.flatMap(env.selectN(_, env.cfg.shuffleNPassive))
                   } yield send(
                     node,
                     ActiveProtocol.Shuffle(env.myself, env.myself, active, passive, TimeToLive(env.cfg.shuffleTTL))
                   )
               }
      } yield task.as(nodes.size)).commit.flatten
    }

}

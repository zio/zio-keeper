package zio.keeper.swim

import java.util.concurrent.TimeUnit

import zio.clock.Clock
import zio.duration.Duration
import zio.keeper.NodeAddress
import zio.stm.{ STM, TMap, TQueue }
import zio.{ UIO, URIO, ZIO, ZLayer }
import zio.clock._

object SuspicionTimeout {

  trait Service {
    def registerTimeout[R, A](node: NodeAddress)(action: URIO[R, A]): ZIO[R, Unit, A]
    def cancelTimeout(node: NodeAddress): UIO[Unit]
    def incomingSuspect(node: NodeAddress, from: NodeAddress): UIO[Unit]
  }

  def registerTimeout[R, A](node: NodeAddress)(action: URIO[R, A]): ZIO[R with SuspicionTimeout, Unit, A] =
    ZIO.accessM[SuspicionTimeout with R](_.get.registerTimeout(node)(action))

  def incomingSuspect(node: NodeAddress, from: NodeAddress): URIO[SuspicionTimeout, Unit] =
    ZIO.accessM[SuspicionTimeout](_.get.incomingSuspect(node, from))

  private case class SuspicionTimeoutEntry(
    start: Long,
    min: Duration,
    max: Duration,
    confirmations: Set[NodeAddress],
    queue: TQueue[Boolean]
  )

  val live = ZLayer.fromEffect(
    TMap
      .empty[NodeAddress, SuspicionTimeoutEntry]
      .commit
      .zip(ZIO.environment[Clock with Nodes])
      .map {
        case (store, env) =>
          val nodes                      = env.get[Nodes.Service]
          val clock                      = env.get[Clock.Service]
          val probeInterval: Duration    = Duration.Zero
          val suspicionAlpha: Int        = 0
          val suspicionBeta: Int         = 0
          val requiredConfirmations: Int = 0
          new Service {

            override def cancelTimeout(node: NodeAddress): UIO[Unit] =
              store
                .get(node)
                .flatMap[Any, Nothing, Unit] {
                  case Some(entry) =>
                    entry.queue.offer(false)
                  case None => STM.unit
                }
                .commit
                .unit

            override def registerTimeout[R, A](node: NodeAddress)(action: URIO[R, A]): ZIO[R, Unit, A] =
              for {
                queue <- TQueue
                          .bounded[Boolean](1)
                          .commit
                numberOfNodes <- nodes.numberOfNodes
                currentTime   <- clock.currentTime(TimeUnit.MILLISECONDS)
                nodeScale     = math.max(1.0, math.log10(math.max(1.0, numberOfNodes.toDouble)))
                min           = probeInterval * suspicionAlpha.toDouble * nodeScale
                max           = min * suspicionBeta.toDouble
                _             <- store.put(node, SuspicionTimeoutEntry(currentTime, min, max, Set.empty, queue)).commit
                result        <- scheduleAction(node)(action)
              } yield result

            private def scheduleAction[R, A](node: NodeAddress)(action: URIO[R, A]): ZIO[R, Unit, A] =
              for {
                maybeEntry <- store.get(node).commit
                result <- maybeEntry match {
                           case Some(entry) =>
                             calculateTimeout(
                               entry.start,
                               entry.max,
                               entry.min,
                               entry.confirmations.size,
                               requiredConfirmations
                             ).provide(env)
                               .flatMap(timeout => clock.sleep(timeout) *> action <* store.delete(node).commit)
                               .race(
                                 ZIO.ifM(entry.queue.take.commit)(scheduleAction(node)(action), ZIO.fail(()))
                               )
                           case None =>
                             //this mean that already cancelled
                             ZIO.fail(())
                         }
              } yield result

            override def incomingSuspect(node: NodeAddress, from: NodeAddress): UIO[Unit] =
              store
                .get(node)
                .flatMap {
                  case Some(entry) if !entry.confirmations.contains(from) =>
                    store.put(node, entry.copy(confirmations = entry.confirmations + from)) *>
                      entry.queue.offer(true)
                  case _ => STM.unit
                }
                .commit
          }
      }
  )

  private def calculateTimeout(
    start: Long,
    max: Duration,
    min: Duration,
    k: Int,
    c: Int
  ): URIO[Clock, Duration] =
    currentTime(TimeUnit.MILLISECONDS).map(currentTime => {
      val elapsed = currentTime - start
      val frac    = math.log(c.toDouble + 1.0) / math.log(k.toDouble + 1.0)
      val raw     = max.toMillis - frac * (max.toMillis - min.toMillis)
      val timeout = math.floor(raw).toLong
      if (timeout < min.toMillis) {
        Duration(min.toMillis - elapsed, TimeUnit.MILLISECONDS)
      } else {
        Duration(timeout - elapsed, TimeUnit.MILLISECONDS)
      }

    })

}

package zio.keeper.swim

import java.util.concurrent.TimeUnit

import zio.clock.{ Clock, _ }
import zio.duration.Duration
import zio.keeper.SwimError.SuspicionTimeoutCancelled
import zio.keeper.{ Error, NodeAddress }
import zio.logging.{ Logger, Logging }
import zio.stm.{ STM, TMap, TQueue }
import zio.{ UIO, URIO, ZIO, ZLayer }

object SuspicionTimeout {

  trait Service {
    def registerTimeout[R, A](node: NodeAddress)(action: ZIO[R, Error, A]): ZIO[R, Error, A]
    def cancelTimeout(node: NodeAddress): UIO[Unit]
    def incomingSuspect(node: NodeAddress, from: NodeAddress): UIO[Unit]
  }

  def registerTimeout[R, A](node: NodeAddress)(action: ZIO[R, Error, A]): ZIO[R with SuspicionTimeout, Error, A] =
    ZIO.accessM[SuspicionTimeout with R](_.get.registerTimeout(node)(action))

  def incomingSuspect(node: NodeAddress, from: NodeAddress): URIO[SuspicionTimeout, Unit] =
    ZIO.accessM[SuspicionTimeout](_.get.incomingSuspect(node, from))

  def cancelTimeout(node: NodeAddress): URIO[SuspicionTimeout, Unit] =
    ZIO.accessM[SuspicionTimeout](_.get.cancelTimeout(node))

  private case class SuspicionTimeoutEntry(
    start: Long,
    min: Duration,
    max: Duration,
    confirmations: Set[NodeAddress],
    queue: TQueue[Boolean]
  )

  def live(
    protocolInterval: Duration,
    suspicionAlpha: Int,
    suspicionBeta: Int,
    suspicionRequiredConfirmations: Int
  ): ZLayer[Clock with Nodes with Logging, Nothing, SuspicionTimeout] = ZLayer.fromEffect(
    TMap
      .empty[NodeAddress, SuspicionTimeoutEntry]
      .commit
      .zip(ZIO.environment[Clock with Nodes with Logging])
      .map {
        case (store, env) =>
          val nodes  = env.get[Nodes.Service]
          val clock  = env.get[Clock.Service]
          val logger = env.get[Logger[String]]

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

            override def registerTimeout[R, A](node: NodeAddress)(action: ZIO[R, Error, A]): ZIO[R, Error, A] =
              for {
                queue <- TQueue
                          .bounded[Boolean](1)
                          .commit
                numberOfNodes <- nodes.numberOfNodes
                currentTime   <- clock.currentTime(TimeUnit.MILLISECONDS)
                nodeScale     = math.max(1.0, math.log10(math.max(1.0, numberOfNodes.toDouble)))
                min           = protocolInterval * suspicionAlpha.toDouble * nodeScale
                max           = min * suspicionBeta.toDouble
                _             <- store.put(node, SuspicionTimeoutEntry(currentTime, min, max, Set.empty, queue)).commit
                result        <- scheduleAction(node)(action)
              } yield result

            private def scheduleAction[R, A](node: NodeAddress)(action: ZIO[R, Error, A]): ZIO[R, Error, A] =
              for {
                maybeEntry <- store.get(node).commit
                result <- maybeEntry match {
                           case Some(entry) =>
                             calculateTimeout(
                               entry.start,
                               entry.max,
                               entry.min,
                               suspicionRequiredConfirmations,
                               entry.confirmations.size
                             ).provide(env)
                               .tap(
                                 timeout =>
                                   logger.info(s"schedule suspicious for $node with timeout: ${timeout.toMillis} ms")
                               )
                               .flatMap(timeout => clock.sleep(timeout) *> action <* store.delete(node).commit)
                               .raceFirst(
                                 ZIO.ifM(entry.queue.take.commit)(
                                   scheduleAction(node)(action),
                                   logger.info(s"suspicious timeout for $node has been cancelled") *>
                                     ZIO.fail(SuspicionTimeoutCancelled(node))
                                 )
                               )
                           case None =>
                             //this mean that already cancelled
                             ZIO.fail(SuspicionTimeoutCancelled(node))
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

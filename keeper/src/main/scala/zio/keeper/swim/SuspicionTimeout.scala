package zio.keeper.swim

import zio.clock.Clock
import zio.duration.Duration
import zio.keeper.NodeAddress
import zio.stm.{STM, TMap, TQueue}
import zio.{UIO, URIO, ZIO, ZLayer}

object SuspicionTimeout {

  trait Service {
    def registerTimeout[R, E, A](node: NodeAddress)(action: ZIO[R, E, A]): ZIO[R, E, A]
    def incomingSuspect(node: NodeAddress, from: NodeAddress): UIO[Unit]
  }

  def registerTimeout[R, E, A](node: NodeAddress)(action: ZIO[R, E, A]): ZIO[R with SuspicionTimeout, E, A] =
    ZIO.accessM[SuspicionTimeout with R](_.get.registerTimeout(node)(action))

  def incomingSuspect(node: NodeAddress, from: NodeAddress): URIO[SuspicionTimeout, Unit] =
    ZIO.accessM[SuspicionTimeout](_.get.incomingSuspect(node, from))

  case class SuspicionTimeoutEntry(confirmations: Set[NodeAddress], queue: TQueue[Unit])

  val live = ZLayer.fromEffect(
    TMap
      .empty[NodeAddress, SuspicionTimeoutEntry]
      .commit
      .zip(ZIO.environment[Clock])
      .map {
        case (store, clock) =>
          new Service {

            override def registerTimeout[R, E, A](node: NodeAddress)(action: ZIO[R, E, A]): ZIO[R, E, A] =
              TQueue
                .bounded[Unit](1)
                .commit
                .map(queue => SuspicionTimeoutEntry(Set.empty, queue))
                .tap(entry => store.put(node, entry).commit) *> scheduleAction(node)(action)

            private def scheduleAction[R, E, A](node: NodeAddress)(action: ZIO[R, E, A]): ZIO[R, E, A] =
              store
                .get(node)
                .commit
                .map(_.get)
                .flatMap(
                  entry =>
                    calculateTimeout
                      .flatMap(t => ZIO.accessM[R](env => action.provide(env).delay(t).provide(clock)))
                      .race(
                        entry.queue.take.commit *> scheduleAction(node)(action)
                      )
                )

            override def incomingSuspect(node: NodeAddress, from: NodeAddress): UIO[Unit] =
              store
                .get(node)
                .flatMap {
                  case Some(entry) if !entry.confirmations.contains(from) =>
                    store.put(node, entry.copy(confirmations = entry.confirmations + from)) *>
                      entry.queue.offer(())
                  case _ => STM.unit
                }
                .commit
          }
      }
  )

  private def calculateTimeout: UIO[Duration] = ???

}

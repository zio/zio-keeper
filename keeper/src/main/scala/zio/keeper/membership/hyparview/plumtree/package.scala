package zio.keeper.membership.hyparview

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.log
import zio.logging.Logging
import zio.stream._
import zio.keeper.NodeAddress
import zio.keeper.membership.hyparview.ActiveProtocol.Prune
import zio.keeper.membership.hyparview.ActiveProtocol.Graft

package object plumtree {

  type PeerState = Has[PeerState.Service]

  def sendGraft[R <: PeerState with PeerService with Clock with Logging, E](
    stream: ZStream[R, E, Either[(UUID, Int, NodeAddress), (UUID, Int, NodeAddress)]],
    initialDelay: Duration,
    sendSchedule: Schedule[R, Unit, _],
    optimizeThreshold: Int
  ): ZStream[R, E, Unit] =
    stream.groupByKey {
      case Left((uuid, _, _))  => uuid
      case Right((uuid, _, _)) => uuid
    } {
      case (uuid, events) =>
        ZStream.fromEffect(ZIO.sleep(initialDelay)) ++
          ZStream.fromEffect(Ref.make(Vector.empty[(NodeAddress, Int)])).flatMap { candidates =>
            val go: ZIO[R, Nothing, Unit] =
              candidates
                .modify { old =>
                  val sorted = old.sortBy(_._2)
                  (sorted.headOption.map(_._1), sorted.drop(1))
                }
                .flatMap(
                  _.fold[URIO[PeerState with PeerService with Logging, Unit]](
                    ZIO.unit
                  )(
                    next =>
                      PeerState.addToEagerPeers(next).commit *>
                        PeerService
                          .send(next, ActiveProtocol.Graft(uuid))
                          .foldCauseM(
                            log.error(s"Failed to send Graft message for $uuid to $next", _),
                            _ => log.debug(s"Sent Graft message for $uuid to $next")
                          )
                  )
                )

            val processEvents: ZIO[R, E, Unit] = events.foreachWhile {
              case Left((uuid, round, winner)) =>
                candidates
                  .modify(old => (old, Vector.empty))
                  .flatMap { remainingCandidates =>
                    remainingCandidates.sortBy(_._2).headOption.fold[URIO[R, Unit]](ZIO.unit) {
                      case (candidate, candidateRound) =>
                        // we have a candidate that has a shorter connection than the winner
                        if (round - candidateRound >= optimizeThreshold) {
                          (PeerState.moveToLazyPeers(winner) *> PeerState.addToEagerPeers(candidate)).commit *>
                            PeerService
                              .send(winner, Prune)
                              .foldCauseM(
                                log.error(s"Failed to send Prune message to $winner", _),
                                _ => log.debug(s"Sent Prune message to $winner")
                              ) *>
                            PeerService
                              .send(candidate, Graft(uuid))
                              .foldCauseM(
                                log.error(s"Failed to send Graft message for $uuid to $candidate", _),
                                _ => log.debug(s"Sent Graft message for $uuid to $candidate")
                              )
                        } else ZIO.unit
                    }
                  }
                  .as(false)
              case Right((_, round, candidate)) => candidates.update(_ :+ ((candidate, round))).as(true)
            }
            ZStream.fromEffect(go.repeat(sendSchedule).race[R, E, Any](processEvents).unit)
          }
    }

  def sendIHave[R <: Logging with PeerService with Clock, E](
    stream: Stream[E, (NodeAddress, UUID, Int)],
    schedule: Schedule[R, Option[List[UUID]], _],
    maxMessageSize: Long = 12
  ): ZStream[R, E, Unit] =
    stream.groupByKey(_._1) {
      case (target, group) =>
        ZStream.fromEffect {
          group
            .map { case (_, uuid, round) => (uuid, round) }
            .aggregateAsyncWithin(
              ZSink.collectAllN[(UUID, Int)](maxMessageSize),
              schedule.contramap[Option[List[(UUID, Int)]]](_.map(_.map(_._1)))
            )
            .foreach {
              case xs @ ::(_, _) =>
                PeerService
                  .send(target, ActiveProtocol.IHave(xs))
                  .foldCauseM(
                    log.error("Failed sending IHave message", _),
                    _ => log.debug("Sent IHave message")
                  )
              case _ =>
                ZIO.unit
            }
        }
    }

}

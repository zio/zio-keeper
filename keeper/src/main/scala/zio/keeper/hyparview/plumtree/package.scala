package zio.keeper.hyparview

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.log
import zio.logging.Logging
import zio.stream._
import zio.keeper.NodeAddress
import zio.keeper.hyparview.Message.{ Graft, IHave, Prune }

package object plumtree {

  type PeerState = Has[PeerState.Service]

  def sendGraft[R <: PeerState with PeerService with Clock with Logging, E](
    stream: ZStream[R, E, Either[(UUID, Round, NodeAddress), (UUID, Round, NodeAddress)]],
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
          ZStream.fromEffect(Ref.make(Vector.empty[(NodeAddress, Round)])).flatMap { candidates =>
            val go: ZIO[R, Nothing, Unit] =
              candidates
                .modify { old =>
                  val sorted = old.sortBy(_._2.value)
                  (sorted.headOption.map(_._1), sorted.drop(1))
                }
                .flatMap(
                  _.fold[URIO[PeerState with PeerService with Logging, Unit]](
                    ZIO.unit
                  )(
                    next =>
                      PeerState.addToEagerPeers(next).commit *>
                        PeerService
                          .send(next, Graft(uuid))
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
                        if (round.value - candidateRound.value >= optimizeThreshold) {
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
    stream: Stream[E, (NodeAddress, UUID, Round)],
    schedule: Schedule[R, Chunk[IHave], _],
    maxMessageSize: Int = 12
  ): ZStream[R, E, Unit] =
    stream.groupByKey(_._1) {
      case (target, group) =>
        ZStream.fromEffect {
          group
            .map { case (_, uuid, round) => (uuid, round) }
            .aggregateAsyncWithin(
              ZTransducer.collectAllN(maxMessageSize).map(xs => IHave(Chunk.fromIterable(xs))),
              schedule
            )
            .foreach { iHave =>
              ZIO.when(iHave.messages.nonEmpty) {
                PeerService
                  .send(target, iHave)
                  .foldCauseM(
                    log.error("Failed sending IHave message", _),
                    _ => log.debug("Sent IHave message")
                  )
              }
            }
        }
    }

}

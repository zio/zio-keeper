package zio.keeper.membership.hyparview

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.log
import zio.logging.Logging.Logging
import zio.stream._
import zio.keeper.{ NodeAddress, SendError }

package object plumtree {

  type PeerState = Has[PeerState.Service]

  def sendGraft[R, E](
    stream: ZStream[R, E, Either[UUID, (UUID, NodeAddress)]],
    initialDelay: Duration,
    sendSchedule: Schedule[R, Unit, _]
  ) =
    stream.groupByKey {
      case Left(uuid)       => uuid
      case Right((uuid, _)) => uuid
    } {
      case (uuid, events) =>
        ZStream.fromEffect(ZIO.sleep(initialDelay)) ++
          ZStream.fromEffect(Ref.make(Vector.empty[NodeAddress])).flatMap { candidates =>
            val go =
              candidates
                .modify(old => (old.headOption, old.drop(1)))
                .flatMap(
                  _.fold[ZIO[PeerService with Logging, SendError, Unit]](
                    ZIO.unit
                  )(
                    next =>
                      PeerService
                        .send(next, ActiveProtocol.Graft(uuid))
                        .foldCauseM(
                          log.error(s"Failed to send Graft message for $uuid to $next", _),
                          _ => log.debug(s"Sent Graft message for $uuid to $next")
                        )
                  )
                )

            val processEvents = events.foreachWhile {
              case Left(_)               => candidates.set(Vector.empty).as(false)
              case Right((_, candidate)) => candidates.update(_ :+ candidate).as(true)
            }
            ZStream.fromEffect(go.repeat(sendSchedule).race(processEvents))
          }
    }

  def sendIHave[R, E](
    stream: Stream[E, (NodeAddress, UUID)],
    schedule: Schedule[R, Option[List[UUID]], _],
    maxMessageSize: Long = 12
  ): ZStream[Logging with PeerService with Clock with R, E, Unit] =
    stream.groupByKey(_._1) {
      case (target, group) =>
        ZStream.fromEffect {
          group.map(_._2).aggregateAsyncWithin(ZSink.collectAllN[UUID](maxMessageSize), schedule).foreach {
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

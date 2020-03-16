package zio.membership.hyparview

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.stream._
import zio.membership.SendError

package object plumtree {

  type PeerState[A] = Has[PeerState.Service[A]]

  def sendGraft[T: Tagged, R, E](
    stream: ZStream[R, E, Either[UUID, (UUID, T)]],
    initialDelay: Duration,
    sendSchedule: Schedule[R, Unit, _]
  ) =
    stream.groupByKey {
      case Left(uuid)       => uuid
      case Right((uuid, _)) => uuid
    } {
      case (uuid, events) =>
        ZStream.fromEffect(ZIO.sleep(initialDelay)) ++
          ZStream.fromEffect(Ref.make(Vector.empty[T])).flatMap { candidates =>
            val go =
              candidates
                .modify(old => (old.headOption, old.drop(1)))
                .flatMap(
                  _.fold[ZIO[PeerService[T] with Logging, SendError, Unit]](
                    ZIO.unit
                  )(
                    next =>
                      PeerService
                        .send(next, ActiveProtocol.Graft(uuid))
                        .foldCauseM(
//                          e => logError(s"Failed to send Graft message for $uuid to $next", e),
                          logError,
                          _ => logDebug(s"Sent Graft message for $uuid to $next")
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

  def sendIHave[T: Tagged, R, E](
    stream: Stream[E, (T, UUID)],
    schedule: Schedule[R, Option[List[UUID]], _],
    maxMessageSize: Long = 12
  ): ZStream[Logging with PeerService[T] with Clock with R, E, Unit] =
    stream.groupByKey(_._1) {
      case (target, group) =>
        ZStream.fromEffect {
          group.map(_._2).aggregateAsyncWithin(ZSink.collectAllN[UUID](maxMessageSize), schedule).foreach {
            case xs @ ::(_, _) =>
              PeerService
                .send(target, ActiveProtocol.IHave(xs))
                .foldCauseM(
//                  e => logError("Failed sending IHave message", e),
                  logError,
                  _ => logDebug("Sent IHave message")
                )
            case _ =>
              ZIO.unit
          }
        }
    }

}

package zio.keeper.swim

import zio.duration.Duration
import zio.stm.TRef
import zio.{ UIO, ULayer, URIO, ZIO, ZLayer }

/**
 * LHM is a saturating counter, with a max value S and min value zero, meaning it will not
 * increase above S or decrease below zero.
 *
 * The following events cause the specified changes to the LHM counter:
 * - Successful probe (ping or ping-req with ack): -1
 * - Failed probe +1
 * - Refuting a suspect message about self: +1
 * - Probe with missed nack: +1
 */
object LocalHealthMultiplier {

  trait Service {
    def increase: UIO[Unit]
    def decrease: UIO[Unit]
    def scaleTimeout(timeout: Duration): UIO[Duration]
  }

  def increase: URIO[LocalHealthMultiplier, Unit] =
    ZIO.accessM[LocalHealthMultiplier](_.get.increase)

  def decrease: URIO[LocalHealthMultiplier, Unit] =
    ZIO.accessM[LocalHealthMultiplier](_.get.decrease)

  def scaleTimeout(timeout: Duration): URIO[LocalHealthMultiplier, Duration] =
    ZIO.accessM[LocalHealthMultiplier](_.get.scaleTimeout(timeout))

  def live(max: Int): ULayer[LocalHealthMultiplier] =
    ZLayer.fromEffect(
      TRef
        .makeCommit(0)
        .map(
          ref =>
            new Service {

              override def increase: UIO[Unit] =
                ref.update(current => math.min(current + 1, max)).commit

              override def decrease: UIO[Unit] =
                ref.update(current => math.max(current - 1, 0)).commit

              override def scaleTimeout(timeout: Duration): UIO[Duration] =
                ref.get.commit.map(score => timeout.multipliedBy((score + 1).toLong))
            }
        )
    )

}

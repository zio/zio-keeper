package zio.keeper.swim

import zio.duration.Duration
import zio.stm.TRef
import zio.{ UIO, ULayer, URIO, ZIO, ZLayer }

object LocalHealthAwareness {

  trait Service {
    def increase: UIO[Unit]
    def decrease: UIO[Unit]
    def scaleTimeout(timeout: Duration): UIO[Duration]
  }

  def increase: URIO[LocalHealthAwareness, Unit] =
    ZIO.accessM[LocalHealthAwareness](_.get.increase)

  def decrease: URIO[LocalHealthAwareness, Unit] =
    ZIO.accessM[LocalHealthAwareness](_.get.decrease)

  def scaleTimeout(timeout: Duration): URIO[LocalHealthAwareness, Duration] =
    ZIO.accessM[LocalHealthAwareness](_.get.scaleTimeout(timeout))

  def live(max: Int): ULayer[LocalHealthAwareness] =
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
                ref.get.commit.map(score => timeout * (score.toDouble + 1.0))
            }
        )
    )

}

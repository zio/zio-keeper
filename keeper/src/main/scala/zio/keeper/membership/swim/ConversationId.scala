package zio.keeper.membership.swim

import zio._

object ConversationId {

  trait Service {
    val next: UIO[Long]
  }

  val next: URIO[ConversationId, Long] =
    ZIO.accessM[ConversationId](_.get.next)

  def live: ULayer[ConversationId] =
    ZLayer.fromEffect(
      Ref
        .make[Long](0)
        .map(
          ref =>
            new ConversationId.Service {

              override val next: zio.UIO[Long] =
                ref.updateAndGet(_ + 1)
            }
        )
    )
}

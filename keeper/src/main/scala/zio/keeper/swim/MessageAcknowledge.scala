package zio.keeper.swim

import zio.stm.TSet
import zio._

object MessageAcknowledge {

  trait Service {
    def ack(conversationId: Long): UIO[Unit]
    def register(conversationId: Long): UIO[Unit]
    def isCompleted(conversationId: Long): UIO[Boolean]
  }

  def ack(conversationId: Long): URIO[MessageAcknowledge, Unit] =
    ZIO.accessM[MessageAcknowledge](_.get.ack(conversationId))

  def register(conversationId: Long): URIO[MessageAcknowledge, Unit] =
    ZIO.accessM[MessageAcknowledge](_.get.register(conversationId))

  def isCompleted(conversationId: Long): URIO[MessageAcknowledge, Boolean] =
    ZIO.accessM[MessageAcknowledge](_.get.isCompleted(conversationId))

  val live: ULayer[Has[Service]] =
    ZLayer.fromEffect(
      TSet
        .empty[Long]
        .commit
        .map(
          pendingAcks =>
            new Service {

              override def ack(conversationId: Long): UIO[Unit] =
                pendingAcks.delete(conversationId).commit

              override def register(conversationId: Long): UIO[Unit] =
                pendingAcks.put(conversationId).commit

              override def isCompleted(conversationId: Long): UIO[Boolean] =
                pendingAcks.contains(conversationId).map(!_).commit
            }
        )
    )

}

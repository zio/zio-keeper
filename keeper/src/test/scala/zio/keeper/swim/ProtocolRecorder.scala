package zio.keeper.swim

import zio._
import zio.clock.Clock
import zio.logging.Logging
import zio.stream.ZStream

object ProtocolRecorder {
  type ProtocolRecorder[A] = Has[ProtocolRecorder.Service[A]]

  trait Service[A] {
    def withBehavior(pf: PartialFunction[Message.Direct[A], Message[A]]): UIO[Service[A]]
    def collectN[B](n: Long)(pr: PartialFunction[Message[A], B]): UIO[List[B]]
    def send(msg: Message.Direct[A]): IO[zio.keeper.Error, Message[A]]
  }

  def apply[A: Tag](
    pf: PartialFunction[Message.Direct[A], Message[A]] = PartialFunction.empty
  ): ZIO[ProtocolRecorder[A], Nothing, Service[A]] =
    ZIO.accessM[ProtocolRecorder[A]](recorder => recorder.get.withBehavior(pf))

  def make[R, E, A: Tag](
    protocolFactory: ZIO[R, E, Protocol[A]]
  ): ZLayer[Clock with Logging with Nodes with R, E, ProtocolRecorder[A]] =
    ZLayer.fromEffect {
      for {
        behaviorRef  <- Ref.make[PartialFunction[Message.Direct[A], Message[A]]](PartialFunction.empty)
        protocol     <- protocolFactory
        messageQueue <- ZQueue.bounded[Message[A]](100)
        _            <- protocol.produceMessages.foreach(consumeMessages(messageQueue, _, behaviorRef, protocol)).fork
        stream       = ZStream.fromQueue(messageQueue)
      } yield new Service[A] {

        override def withBehavior(pf: PartialFunction[Message.Direct[A], Message[A]]): UIO[Service[A]] =
          behaviorRef.set(pf).as(this)

        override def collectN[B](n: Long)(pf: PartialFunction[Message[A], B]): UIO[List[B]] =
          stream.collect(pf).take(n).runCollect.map(_.toList)

        override def send(msg: Message.Direct[A]): IO[zio.keeper.Error, Message[A]] =
          protocol.onMessage(msg)
      }
    }

  private def consumeMessages[A](
    messageQueue: zio.Queue[Message[A]],
    message: Message[A],
    behaviorRef: Ref[PartialFunction[Message.Direct[A], Message[A]]],
    protocol: Protocol[A]
  ): ZIO[Clock with Logging with Nodes, zio.keeper.Error, Unit] =
    message match {
      case Message.WithTimeout(message, action, timeout) =>
        consumeMessages(messageQueue, message, behaviorRef, protocol).unit *>
          action.delay(timeout).flatMap(consumeMessages(messageQueue, _, behaviorRef, protocol)).unit
      case md: Message.Direct[A] =>
        messageQueue.offer(md) *>
          behaviorRef.get.flatMap { fn =>
            ZIO.whenCase(fn.lift(md)) {
              case Some(d: Message.Direct[A]) => protocol.onMessage(d)
            }
          }
      case msg =>
        messageQueue.offer(msg).unit
    }
}
